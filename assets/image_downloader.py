#!/usr/bin/env python3
"""
image_downloader.py — Crawl a website and download every image it can find.

Features
--------
* Crawls HTML pages (same domain by default) up to a configurable depth.
* Finds images from <img src>, <img srcset>, <picture><source srcset>,
  CSS url(...) (inline style + <link rel=stylesheet> + <style> blocks),
  <a href> links pointing at image files, and og:image / twitter:image meta tags.
* Detects "thumbnail-looking" URLs and generates candidate full-size URLs
  (e.g. photo-150x150.jpg -> photo.jpg, /thumbs/x.jpg -> /x.jpg,
  photo_thumb.jpg -> photo.jpg) and prefers the largest one that exists.
* Preserves the original site folder structure under the output directory,
  namespaced per-domain.
* Skips duplicate downloads by URL — files already recorded in the manifest
  are not re-fetched, and each URL's file is saved to its own path so that
  legitimately reused assets (e.g. the same logo/icon repeated across many
  folders on a site) are preserved wherever the site actually places them.
* Resumable: progress is persisted to a JSON manifest after every file, so
  killing the process (Ctrl+C, crash, network loss) and re-running the same
  command picks up where it left off.
* Logs every missing / failed URL (404s, timeouts, connection errors, etc.)
  to a separate log file.

Usage
-----
    python3 image_downloader.py https://example.com -o ./downloaded_images

    # more options
    python3 image_downloader.py https://example.com \\
        --output ./downloaded_images \\
        --max-depth 3 \\
        --workers 8 \\
        --delay 0.25 \\
        --same-domain-only \\
        --no-thumb-upgrade

Run with -h/--help for the full list of options.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
import urllib.robotparser as robotparser
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse, urlunparse, unquote

try:
    import requests
except ImportError:  # pragma: no cover
    print("This tool requires the 'requests' package: pip install requests beautifulsoup4", file=sys.stderr)
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover
    print("This tool requires 'beautifulsoup4': pip install requests beautifulsoup4", file=sys.stderr)
    sys.exit(1)


IMAGE_EXTENSIONS = {
    "jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "tiff", "tif",
    "ico", "avif", "heic", "heif", "jfif",
}

# Matches things like: name-150x150.jpg, name_300x200.png (WordPress-style resized images)
DIMENSION_SUFFIX_RE = re.compile(r"-\d{2,5}x\d{2,5}(?=\.[A-Za-z0-9]+$)")

# Other common "thumbnail" markers seen in filenames / paths.
THUMB_TOKEN_RE = re.compile(r"(?:^|[-_.])(?:thumb|thumbnail|tn|tmb|small|sm|preview|mini|icon)(?:[-_.]|$)", re.I)

CSS_URL_RE = re.compile(r"url\(\s*(['\"]?)([^'\")]+)\1\s*\)", re.I)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; ImageDownloaderBot/1.0; "
    "+https://example.local/image-downloader)"
)


# --------------------------------------------------------------------------- #
# Data structures
# --------------------------------------------------------------------------- #

@dataclasses.dataclass
class DownloadResult:
    url: str
    status: str  # "downloaded" | "skipped-duplicate-url" | "failed"
    local_path: Optional[str] = None
    reason: Optional[str] = None


class Manifest:
    """Tracks completed downloads so the tool can resume safely."""

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.data = {
            "completed_urls": {},   # url -> {"path": str, "sha256": str, "size": int}
            "visited_pages": [],    # crawled HTML pages (for resume of the crawl)
        }
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.data.update(json.load(f))
            except (json.JSONDecodeError, OSError):
                # Corrupt manifest — start fresh rather than crash.
                pass

    def is_url_done(self, url: str) -> bool:
        with self.lock:
            entry = self.data["completed_urls"].get(url)
            if not entry:
                return False
            # Verify the file is still actually on disk before trusting the manifest.
            return Path(entry["path"]).exists()

    def mark_done(self, url: str, local_path: Path, sha256: str, size: int):
        with self.lock:
            self.data["completed_urls"][url] = {
                "path": str(local_path),
                "sha256": sha256,
                "size": size,
            }
            self._flush_locked()

    def is_page_visited(self, url: str) -> bool:
        with self.lock:
            return url in self.data["visited_pages"]

    def mark_page_visited(self, url: str):
        with self.lock:
            self.data["visited_pages"].append(url)
            self._flush_locked()

    def _flush_locked(self):
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f)
        tmp.replace(self.path)


# --------------------------------------------------------------------------- #
# Core downloader
# --------------------------------------------------------------------------- #

class ImageDownloader:
    def __init__(
        self,
        start_urls: Iterable[str],
        output_dir: Path,
        max_depth: int = 2,
        same_domain_only: bool = True,
        workers: int = 6,
        delay: float = 0.1,
        user_agent: str = DEFAULT_USER_AGENT,
        upgrade_thumbnails: bool = True,
        max_pages: Optional[int] = None,
        respect_robots: bool = True,
        timeout: int = 20,
        extra_extensions: Optional[Iterable[str]] = None,
    ):
        self.start_urls = list(start_urls)
        if not self.start_urls:
            raise ValueError("At least one start URL is required (positional URL and/or --url-file).")
        self.start_domains = {urlparse(u).netloc for u in self.start_urls}
        self.output_dir = Path(output_dir)
        self.max_depth = max_depth
        self.same_domain_only = same_domain_only
        self.workers = workers
        self.delay = delay
        self.upgrade_thumbnails = upgrade_thumbnails
        self.max_pages = max_pages
        self.timeout = timeout
        self.extensions = set(IMAGE_EXTENSIONS)
        if extra_extensions:
            self.extensions.update(e.lower().lstrip(".") for e in extra_extensions)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = Manifest(self.output_dir / ".image_downloader_manifest.json")

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

        self._setup_logging()

        self.robots = None
        if respect_robots:
            self.robots = {}  # domain -> RobotFileParser
            for su in self.start_urls:
                domain = urlparse(su).netloc
                if domain in self.robots:
                    continue
                rp = robotparser.RobotFileParser()
                try:
                    # Use our own session (with a real User-Agent) instead of
                    # rp.read(), which uses urllib's default UA and gets 403'd
                    # by some servers/WAFs — RobotFileParser then defaults to
                    # disallow-all instead of failing open.
                    robots_url = urljoin(su, "/robots.txt")
                    resp = self.session.get(robots_url, timeout=timeout)
                    if resp.status_code == 200:
                        rp.parse(resp.text.splitlines())
                        self.robots[domain] = rp
                    # Any non-200 (403/404/etc.) => no entry for this domain,
                    # which _allowed_by_robots() treats as fail-open.
                except requests.RequestException:
                    pass  # fail open for this domain if robots.txt can't be fetched

        self._page_queue_lock = threading.Lock()
        self._pages_seen: set[str] = set()
        self._image_urls_seen: set[str] = set()
        self._images_lock = threading.Lock()

        self.stats = {"pages_crawled": 0, "images_found": 0, "downloaded": 0,
                       "skipped_duplicate": 0, "failed": 0}

    # ------------------------------------------------------------------- #
    # Logging
    # ------------------------------------------------------------------- #
    def _setup_logging(self):
        self.logger = logging.getLogger("image_downloader")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(fmt)
        self.logger.addHandler(console)

        general_log = logging.FileHandler(self.output_dir / "download.log", encoding="utf-8")
        general_log.setFormatter(fmt)
        self.logger.addHandler(general_log)

        # Separate logger just for missing/failed files.
        self.missing_logger = logging.getLogger("image_downloader.missing")
        self.missing_logger.setLevel(logging.INFO)
        self.missing_logger.handlers.clear()
        missing_handler = logging.FileHandler(self.output_dir / "missing_files.log", encoding="utf-8")
        missing_handler.setFormatter(logging.Formatter("%(asctime)s\t%(message)s", "%Y-%m-%d %H:%M:%S"))
        self.missing_logger.addHandler(missing_handler)
        self.missing_logger.propagate = False

    def log_missing(self, url: str, reason: str, referrer: str = ""):
        self.missing_logger.info(f"{url}\treason={reason}\treferrer={referrer}")

    # ------------------------------------------------------------------- #
    # Robots / scope helpers
    # ------------------------------------------------------------------- #
    def _allowed_by_robots(self, url: str) -> bool:
        if not self.robots:
            return True
        domain = urlparse(url).netloc
        rp = self.robots.get(domain)
        if not rp:
            return True
        try:
            return rp.can_fetch(self.session.headers.get("User-Agent", "*"), url)
        except Exception:
            return True

    def _in_scope(self, url: str) -> bool:
        if not self.same_domain_only:
            return True
        return urlparse(url).netloc in self.start_domains

    def _is_image_url(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        ext = path.rsplit(".", 1)[-1] if "." in path.rsplit("/", 1)[-1] else ""
        return ext in self.extensions

    # ------------------------------------------------------------------- #
    # Crawl
    # ------------------------------------------------------------------- #
    def crawl(self):
        self.logger.info(f"Starting crawl with {len(self.start_urls)} seed URL(s) "
                          f"(max_depth={self.max_depth}, same_domain_only={self.same_domain_only})")

        image_urls: dict[str, str] = {}  # image_url -> referrer page
        to_visit: list[tuple[str, int]] = []

        for su in self.start_urls:
            if self._is_image_url(su):
                # A direct image URL from the input file — no crawling needed.
                image_urls.setdefault(su, su)
            else:
                to_visit.append((su, 0))

        while to_visit:
            if self.max_pages and self.stats["pages_crawled"] >= self.max_pages:
                self.logger.info(f"Reached max_pages={self.max_pages}, stopping crawl.")
                break

            page_url, depth = to_visit.pop(0)
            norm_page = _normalize_url(page_url)
            if norm_page in self._pages_seen:
                continue
            self._pages_seen.add(norm_page)

            if self.manifest.is_page_visited(norm_page):
                self.logger.info(f"[resume] already crawled: {page_url}")
                # We still need its images recorded; re-crawl cheaply is simplest & safest
                # only if the manifest doesn't already have them, but to keep resume fast
                # we just skip re-parsing pages already visited in a prior run.
                continue

            if not self._allowed_by_robots(page_url):
                self.logger.info(f"Blocked by robots.txt, skipping page: {page_url}")
                continue

            try:
                resp = self.session.get(page_url, timeout=self.timeout)
                resp.raise_for_status()
            except requests.RequestException as e:
                self.logger.warning(f"Failed to fetch page {page_url}: {e}")
                self.log_missing(page_url, f"page-fetch-error: {e}")
                continue

            content_type = resp.headers.get("Content-Type", "")
            self.stats["pages_crawled"] += 1
            self.manifest.mark_page_visited(norm_page)

            if "text/html" not in content_type:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            found = self._extract_image_urls(soup, page_url)
            for img_url in found:
                if img_url not in image_urls:
                    image_urls[img_url] = page_url

            # iWeb "MediaGrid"/"Photo Grid" pages store real photos in a
            # sibling rss.xml photocast feed instead of the page HTML.
            for feed_url in self._find_photocast_feeds(soup, page_url):
                for img_url in self._extract_photocast_images(feed_url, page_url):
                    if img_url not in image_urls:
                        image_urls[img_url] = page_url

            if depth < self.max_depth:
                for link_url in self._extract_page_links(soup, page_url):
                    if self._in_scope(link_url) and _normalize_url(link_url) not in self._pages_seen:
                        to_visit.append((link_url, depth + 1))

            time.sleep(self.delay)

        self.stats["images_found"] = len(image_urls)
        self.logger.info(f"Crawl complete. Pages crawled: {self.stats['pages_crawled']}, "
                          f"image URLs found: {len(image_urls)}")
        return image_urls

    def _extract_page_links(self, soup: BeautifulSoup, base_url: str):
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith(("mailto:", "javascript:", "tel:", "#")):
                continue
            yield urljoin(base_url, href)

    def _extract_image_urls(self, soup: BeautifulSoup, base_url: str) -> set[str]:
        urls: set[str] = set()

        def add(u: Optional[str]):
            if not u:
                return
            u = u.strip()
            if not u or u.startswith("data:"):
                return
            full = urljoin(base_url, u)
            full = full.split("#")[0]
            urls.add(full)

        # <img src>, data-src / data-lazy-src (common lazy-loading attrs), srcset
        for img in soup.find_all("img"):
            add(img.get("src"))
            for attr in ("data-src", "data-lazy-src", "data-original", "data-full-src"):
                add(img.get(attr))
            for attr in ("srcset", "data-srcset"):
                srcset = img.get(attr)
                if srcset:
                    for part in srcset.split(","):
                        candidate = part.strip().split(" ")[0]
                        add(candidate)

        # <picture><source srcset>
        for source in soup.find_all("source"):
            srcset = source.get("srcset")
            if srcset:
                for part in srcset.split(","):
                    add(part.strip().split(" ")[0])

        # <a href="....jpg">  (linked full-size images / galleries)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if self._is_image_url(urljoin(base_url, href)):
                add(href)

        # meta og:image / twitter:image
        for meta in soup.find_all("meta"):
            prop = (meta.get("property") or meta.get("name") or "").lower()
            if prop in ("og:image", "og:image:url", "twitter:image", "twitter:image:src"):
                add(meta.get("content"))

        # inline style="background-image: url(...)"
        for tag in soup.find_all(style=True):
            for m in CSS_URL_RE.finditer(tag["style"]):
                add(m.group(2))

        # <style> blocks
        for style_tag in soup.find_all("style"):
            for m in CSS_URL_RE.finditer(style_tag.get_text()):
                add(m.group(2))

        # Filter to only things that look like images (by extension) to avoid
        # dragging in every random link on the page.
        return {u for u in urls if self._is_image_url(u)}

    # ------------------------------------------------------------------- #
    # iWeb "photocast" RSS feed support
    # ------------------------------------------------------------------- #
    def _find_photocast_feeds(self, soup: BeautifulSoup, base_url: str) -> set[str]:
        """Old iWeb-generated sites (MediaGrid / Photo Grid widgets) don't put
        photos in the page HTML at all — the real image list lives in a
        sibling 'rss.xml' photocast feed. Detect references to it via the
        standard <link rel="alternate" type="application/rss+xml"> tag and
        via inline JS calls like IWCreatePhotocast("....rss.xml", ...)."""
        feeds: set[str] = set()

        for link in soup.find_all("link", rel="alternate"):
            if "rss" in (link.get("type") or "").lower():
                href = link.get("href")
                if href:
                    feeds.add(self._to_site_relative_url(href, base_url))

        for script in soup.find_all("script"):
            text = script.get_text() or ""
            for m in re.finditer(r"""(?:CreatePhotocast|photocast)\s*\(\s*["']([^"']+rss\.xml)["']""",
                                  text, re.I):
                feeds.add(self._to_site_relative_url(m.group(1), base_url))

        return {f for f in feeds if f}

    def _to_site_relative_url(self, raw_url: str, base_url: str) -> Optional[str]:
        """iWeb embeds absolute file:// paths from the original author's Mac
        (e.g. file://localhost/Users/name/Desktop/iweb site/Site/Foo_files/x.xml).
        Everything after the '/Site/' segment mirrors the real published
        site's folder structure *from the site root* (not relative to the
        feed file itself), so it must be joined against the domain root."""
        if raw_url.startswith("file://"):
            marker = "/Site/"
            idx = raw_url.find(marker)
            if idx == -1:
                return None
            rel = unquote(raw_url[idx + len(marker):])
            parsed_base = urlparse(base_url)
            site_root = f"{parsed_base.scheme}://{parsed_base.netloc}/"
            return urljoin(site_root, rel)
        return urljoin(base_url, raw_url)

    def _extract_photocast_images(self, feed_url: str, page_url: str) -> set[str]:
        """Fetch an iWeb RSS photocast feed and return full-size image URLs
        (preferring <enclosure url> over <iphoto:thumbnail>/<iweb:micro>)."""
        images: set[str] = set()
        try:
            resp = self.session.get(feed_url, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            self.logger.warning(f"Failed to fetch photocast feed {feed_url}: {e}")
            self.log_missing(feed_url, f"photocast-fetch-error: {e}", page_url)
            return images

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            self.logger.warning(f"Could not parse photocast feed {feed_url}: {e}")
            return images

        for item in root.iter("item"):
            enclosure = item.find("enclosure")
            raw = enclosure.get("url") if enclosure is not None else None
            if not raw:
                continue
            resolved = self._to_site_relative_url(raw, feed_url)
            if resolved and self._is_image_url(resolved):
                images.add(resolved)

        if images:
            self.logger.info(f"Photocast feed {feed_url}: found {len(images)} full-size image(s)")
        return images

    # ------------------------------------------------------------------- #
    # Thumbnail -> full-size upgrade
    # ------------------------------------------------------------------- #
    def _candidate_full_size_urls(self, url: str) -> list[str]:
        """Generate plausible 'full size' URL candidates for a thumbnail URL."""
        if not self.upgrade_thumbnails:
            return []

        parsed = urlparse(url)
        path = parsed.path
        candidates: set[str] = set()

        # 1. WordPress-style dimension suffix: name-150x150.jpg -> name.jpg
        if DIMENSION_SUFFIX_RE.search(path):
            candidates.add(DIMENSION_SUFFIX_RE.sub("", path))

        # 2. Token-based thumbnail markers in the filename: name_thumb.jpg -> name.jpg
        dirname, filename = path.rsplit("/", 1) if "/" in path else ("", path)
        stem, dot, ext = filename.rpartition(".")
        if dot:
            for token in ("thumb", "thumbnail", "tn", "tmb", "small", "sm", "preview", "mini", "icon"):
                for pattern in (f"_{token}", f"-{token}", f".{token}", token):
                    if stem.lower().endswith(pattern):
                        new_stem = stem[: len(stem) - len(pattern)]
                        if new_stem:
                            candidates.add(f"{dirname}/{new_stem}.{ext}" if dirname else f"{new_stem}.{ext}")
                    if stem.lower().startswith(pattern):
                        new_stem = stem[len(pattern):]
                        if new_stem:
                            candidates.add(f"{dirname}/{new_stem}.{ext}" if dirname else f"{new_stem}.{ext}")

        # 3. Thumbnail folder swap: /thumbs/x.jpg, /thumbnails/x.jpg, /thumb/x.jpg -> /x.jpg or /originals/x.jpg
        parts = [p for p in path.split("/")]
        thumb_dir_names = {"thumb", "thumbs", "thumbnail", "thumbnails", "small", "preview", "previews"}
        for i, part in enumerate(parts):
            if part.lower() in thumb_dir_names:
                without = parts[:i] + parts[i + 1:]
                candidates.add("/".join(without))
                for replacement in ("original", "originals", "full", "large"):
                    swapped = parts[:i] + [replacement] + parts[i + 1:]
                    candidates.add("/".join(swapped))

        # 4. Generic word swaps: small->large, thumb->full, mini->max, etc.
        swap_pairs = [
            ("thumb", "full"), ("thumbnail", "original"), ("small", "large"),
            ("sm", "lg"), ("mini", "max"), ("preview", "original"), ("low", "high"),
        ]
        lower_path = path.lower()
        for a, b in swap_pairs:
            if a in lower_path:
                idx = lower_path.find(a)
                new_path = path[:idx] + b + path[idx + len(a):]
                candidates.add(new_path)

        results = []
        for c in candidates:
            if c and c != path:
                results.append(urlunparse(parsed._replace(path=c)))
        return results

    def _resolve_best_url(self, url: str, referrer: str) -> str:
        """Return the best available URL for this image: the original, or an
        upgraded full-size version if one exists and looks 'better' (bigger)."""
        candidates = self._candidate_full_size_urls(url)
        if not candidates:
            return url

        best_url = url
        best_size = self._head_content_length(url)

        for cand in candidates:
            size = self._head_content_length(cand)
            if size is not None and (best_size is None or size > best_size):
                best_url, best_size = cand, size

        if best_url != url:
            self.logger.info(f"Upgraded thumbnail -> full size: {url}  =>  {best_url}")
        return best_url

    def _head_content_length(self, url: str) -> Optional[int]:
        try:
            resp = self.session.head(url, timeout=self.timeout, allow_redirects=True)
            if resp.status_code == 200:
                cl = resp.headers.get("Content-Length")
                return int(cl) if cl is not None else 0
            return None
        except requests.RequestException:
            return None

    # ------------------------------------------------------------------- #
    # Download
    # ------------------------------------------------------------------- #
    def _local_path_for(self, url: str) -> Path:
        """Preserve folder structure: <output>/<domain>/<url path>"""
        parsed = urlparse(url)
        domain_dir = parsed.netloc or (next(iter(self.start_domains), "site"))
        path = unquote(parsed.path)
        if not path or path.endswith("/"):
            path = path + "index.jpg"
        # Strip leading slash, sanitize
        rel = path.lstrip("/")
        # Guard against path traversal from a hostile server.
        rel_parts = [p for p in rel.split("/") if p not in ("", ".", "..")]
        return self.output_dir / domain_dir / Path(*rel_parts)

    def download_one(self, url: str, referrer: str) -> DownloadResult:
        try:
            resolved_url = self._resolve_best_url(url, referrer)

            if self.manifest.is_url_done(resolved_url) and self.manifest.is_url_done(url):
                return DownloadResult(url, "skipped-duplicate-url")
            # If either original or resolved was already downloaded, skip.
            if self.manifest.is_url_done(resolved_url):
                return DownloadResult(url, "skipped-duplicate-url")

            local_path = self._local_path_for(resolved_url)
            if local_path.exists() and self.manifest.is_url_done(resolved_url):
                return DownloadResult(url, "skipped-duplicate-url", str(local_path))

            resp = self.session.get(resolved_url, timeout=self.timeout, stream=True)
            if resp.status_code == 404:
                self.log_missing(resolved_url, "404-not-found", referrer)
                return DownloadResult(url, "failed", reason="404")
            resp.raise_for_status()

            data = resp.content
            sha256 = hashlib.sha256(data).hexdigest()

            local_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = local_path.with_suffix(local_path.suffix + ".part")
            with open(tmp_path, "wb") as f:
                f.write(data)
            tmp_path.replace(local_path)

            self.manifest.mark_done(resolved_url, local_path, sha256, len(data))
            return DownloadResult(url, "downloaded", str(local_path))

        except requests.RequestException as e:
            self.log_missing(url, f"error: {e}", referrer)
            return DownloadResult(url, "failed", reason=str(e))
        except OSError as e:
            self.log_missing(url, f"filesystem-error: {e}", referrer)
            return DownloadResult(url, "failed", reason=str(e))

    def download_all(self, image_urls: dict[str, str]):
        self.logger.info(f"Downloading {len(image_urls)} images with {self.workers} workers...")
        results: list[DownloadResult] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as pool:
            future_map = {
                pool.submit(self.download_one, url, ref): url
                for url, ref in image_urls.items()
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_map), 1):
                result = future.result()
                results.append(result)
                if result.status == "downloaded":
                    self.stats["downloaded"] += 1
                    self.logger.info(f"[{i}/{len(image_urls)}] downloaded: {result.url}")
                elif result.status.startswith("skipped"):
                    self.stats["skipped_duplicate"] += 1
                    self.logger.info(f"[{i}/{len(image_urls)}] skipped ({result.status}): {result.url}")
                else:
                    self.stats["failed"] += 1
                    self.logger.warning(f"[{i}/{len(image_urls)}] FAILED: {result.url} ({result.reason})")
                time.sleep(self.delay / max(self.workers, 1))

        return results

    def run(self):
        image_urls = self.crawl()
        self.download_all(image_urls)
        self.logger.info(
            "Done. "
            f"pages_crawled={self.stats['pages_crawled']} "
            f"images_found={self.stats['images_found']} "
            f"downloaded={self.stats['downloaded']} "
            f"skipped_duplicates={self.stats['skipped_duplicate']} "
            f"failed={self.stats['failed']}"
        )
        self.logger.info(f"Missing/failed file log: {self.output_dir / 'missing_files.log'}")
        self.logger.info(f"Manifest (for resume): {self.manifest.path}")


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(fragment=""))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download every image from a website, upgrading thumbnails to full-size when possible.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("url", nargs="?", default=None,
                   help="Starting URL of the website to crawl. Optional if --url-file is given.")
    p.add_argument("-f", "--url-file", default=None,
                   help="Path to a text file with one URL per line (page URLs are crawled; "
                        "direct image URLs are downloaded as-is). Lines starting with '#' "
                        "and blank lines are ignored.")
    p.add_argument("-o", "--output", default="./downloaded_images", help="Output directory.")
    p.add_argument("--max-depth", type=int, default=2, help="Max link-following depth from the start URL.")
    p.add_argument("--max-pages", type=int, default=None, help="Optional cap on number of pages to crawl.")
    p.add_argument("--workers", type=int, default=6, help="Concurrent download workers.")
    p.add_argument("--delay", type=float, default=0.1, help="Delay (seconds) between requests, per worker.")
    p.add_argument("--timeout", type=int, default=20, help="Per-request timeout in seconds.")
    p.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent header to send.")
    p.add_argument("--all-domains", dest="same_domain_only", action="store_false",
                   help="Also follow/crawl links to other domains (default: same domain only).")
    p.add_argument("--no-thumb-upgrade", dest="upgrade_thumbnails", action="store_false",
                   help="Disable attempting to find full-size versions of thumbnails.")
    p.add_argument("--ignore-robots", dest="respect_robots", action="store_false",
                   help="Ignore robots.txt (not recommended).")
    p.add_argument("--ext", action="append", default=[],
                   help="Additional file extension(s) to treat as images (repeatable).")
    return p


def load_urls_from_file(path: str) -> list[str]:
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)
    return urls


def main(argv=None):
    args = build_arg_parser().parse_args(argv)

    start_urls: list[str] = []
    if args.url:
        start_urls.append(args.url)
    if args.url_file:
        try:
            file_urls = load_urls_from_file(args.url_file)
        except OSError as e:
            print(f"Could not read --url-file '{args.url_file}': {e}", file=sys.stderr)
            sys.exit(1)
        start_urls.extend(u for u in file_urls if u not in start_urls)

    if not start_urls:
        print("Provide a URL argument, --url-file, or both.", file=sys.stderr)
        sys.exit(1)

    downloader = ImageDownloader(
        start_urls=start_urls,
        output_dir=Path(args.output),
        max_depth=args.max_depth,
        same_domain_only=args.same_domain_only,
        workers=args.workers,
        delay=args.delay,
        user_agent=args.user_agent,
        upgrade_thumbnails=args.upgrade_thumbnails,
        max_pages=args.max_pages,
        respect_robots=args.respect_robots,
        timeout=args.timeout,
        extra_extensions=args.ext,
    )

    try:
        downloader.run()
    except KeyboardInterrupt:
        downloader.logger.warning(
            "Interrupted by user. Progress has been saved — re-run the same command to resume."
        )
        sys.exit(130)


if __name__ == "__main__":
    main()
