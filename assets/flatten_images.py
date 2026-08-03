#!/usr/bin/env python3
"""
flatten_images.py — Collect every image from a nested folder tree (like the
output of image_downloader.py) into a single flat destination folder.

Duplicate filenames are resolved by keeping the FIRST occurrence found and
skipping every subsequent file with the same name — they're logged to
flatten_skipped.log so nothing is silently lost track of.

Usage
-----
    python3 flatten_images.py ./downloaded_images ./downloaded_images_flat

    # move instead of copy (removes files from the nested tree)
    python3 flatten_images.py ./downloaded_images ./downloaded_images_flat --move

Notes
-----
* "First occurrence" is determined by os.walk() order, which is generally
  alphabetical-by-directory on macOS but not guaranteed — if you care which
  specific copy wins for a given filename, sort/rename source folders first.
* Only files are considered; the manifest/log files created by
  image_downloader.py (.image_downloader_manifest.json, download.log,
  missing_files.log) are skipped automatically.
* Safe to re-run: files already present in the destination are left alone
  and reported as duplicates, so running twice won't fail or overwrite.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

IGNORED_FILES = {
    ".image_downloader_manifest.json",
    "download.log",
    "missing_files.log",
    "flatten_skipped.log",
    ".DS_Store",
    "desktop.ini",
    "Thumbs.db",
}

IMAGE_EXTENSIONS = {
    "jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "tiff", "tif",
    "ico", "avif", "heic", "heif", "jfif",
}


def flatten(source: Path, destination: Path, move: bool = False) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    skipped_log_path = destination / "flatten_skipped.log"

    seen_names: set[str] = {p.name for p in destination.iterdir() if p.is_file()}
    copied = 0
    skipped = 0

    with open(skipped_log_path, "a", encoding="utf-8") as skipped_log:
        for path in sorted(source.rglob("*")):
            if not path.is_file():
                continue
            if path.name in IGNORED_FILES or path.name.endswith(".part"):
                continue
            if path.suffix.lower().lstrip(".") not in IMAGE_EXTENSIONS:
                continue

            if path.name in seen_names:
                skipped += 1
                skipped_log.write(f"{path}\n")
                continue

            dest_path = destination / path.name
            if move:
                shutil.move(str(path), str(dest_path))
            else:
                shutil.copy2(str(path), str(dest_path))

            seen_names.add(path.name)
            copied += 1

    action = "Moved" if move else "Copied"
    print(f"{action} {copied} file(s) into {destination}")
    print(f"Skipped {skipped} duplicate filename(s) — see {skipped_log_path}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Flatten a nested image folder tree into one directory, "
                    "keeping the first copy of each filename and logging duplicates.",
    )
    parser.add_argument("source", help="Source folder to flatten (e.g. ./downloaded_images)")
    parser.add_argument("destination", help="Destination folder for the flattened images")
    parser.add_argument("--move", action="store_true",
                         help="Move files instead of copying (removes them from the source tree)")
    args = parser.parse_args(argv)

    source = Path(args.source).resolve()
    destination = Path(args.destination).resolve()

    if not source.is_dir():
        print(f"Source folder not found: {source}", file=sys.stderr)
        sys.exit(1)
    if destination == source or destination in source.parents or source in destination.parents:
        print("Destination must not be the same as, or nested inside/around, the source folder.",
              file=sys.stderr)
        sys.exit(1)

    flatten(source, destination, move=args.move)


if __name__ == "__main__":
    main()
