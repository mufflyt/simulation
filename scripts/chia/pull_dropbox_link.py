#!/usr/bin/env python3
"""
Materialise a Dropbox shared-link folder to local disk.

The CHIA 2015-2018 files live in a team namespace and are present locally only
as 0-byte com.dropbox.placeholder stubs, so they have to come down over the API.
A shared link sidesteps the namespace problem entirely: list_folder and
get_shared_link_file both accept the link itself as the root.

Every file is hashed with Dropbox's own content_hash algorithm (SHA-256 over
4 MiB blocks, then SHA-256 of the concatenated block hashes) and checked against
the server's value, so a truncated download is caught here rather than three
hours later in DuckDB.

Usage:
  pull_dropbox_link.py --url URL --dest DIR [--include EXT ...] [--list-only]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
import time
import urllib.error
import urllib.request

API = "https://api.dropboxapi.com/2"
CONTENT = "https://content.dropboxapi.com/2"
BLOCK = 4 * 1024 * 1024


def token() -> str:
    auth = json.loads(pathlib.Path.home().joinpath(".config/dbxcli/auth.json").read_text())
    return auth[""]["personal"]


def api(endpoint: str, payload: dict, tok: str, tries: int = 5) -> dict:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{API}/{endpoint}", data=body,
        headers={"Authorization": f"Bearer {tok}",
                 "Content-Type": "application/json"},
    )
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            if exc.code in (429, 500, 503) and attempt < tries - 1:
                time.sleep(2 ** attempt * 2)
                continue
            raise RuntimeError(f"{endpoint} -> HTTP {exc.code}: {detail}") from None
    raise RuntimeError(f"{endpoint}: retries exhausted")


def content_hash(path: pathlib.Path) -> str:
    """Dropbox's content_hash: SHA-256 of the concatenated per-4MiB SHA-256s."""
    outer = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(BLOCK):
            outer.update(hashlib.sha256(chunk).digest())
    return outer.hexdigest()


def walk(url: str, tok: str) -> list[dict]:
    """Every file under the shared link.

    The API rejects recursive=true on a shared link, so descend one folder at a
    time. Paths are relative to the link root, and the returned path_display is
    too, which is exactly what get_shared_link_file wants back.
    """
    files: list[dict] = []
    queue = [""]
    while queue:
        folder = queue.pop(0)
        res = api("files/list_folder",
                  {"path": folder, "shared_link": {"url": url}, "limit": 2000}, tok)
        while True:
            for entry in res["entries"]:
                # Shared-link listings omit path_lower/path_display, so carry the
                # path down from the parent ourselves.
                rel = f"{folder}/{entry['name']}"
                if entry[".tag"] == "file":
                    entry["_rel"] = rel
                    files.append(entry)
                elif entry[".tag"] == "folder":
                    queue.append(rel)
            if not res.get("has_more"):
                break
            res = api("files/list_folder/continue", {"cursor": res["cursor"]}, tok)
    return files


def download(url: str, rel_path: str, dest: pathlib.Path, tok: str,
             expect_hash: str | None, expect_size: int, tries: int = 4) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    arg = json.dumps({"url": url, "path": rel_path})
    for attempt in range(tries):
        req = urllib.request.Request(
            f"{CONTENT}/sharing/get_shared_link_file",
            data=b"",
            headers={"Authorization": f"Bearer {tok}",
                     "Dropbox-API-Arg": arg},
        )
        part = dest.with_suffix(dest.suffix + ".part")
        try:
            with urllib.request.urlopen(req, timeout=600) as resp, part.open("wb") as out:
                got = 0
                mark = time.time()
                while chunk := resp.read(8 << 20):
                    out.write(chunk)
                    got += len(chunk)
                    if time.time() - mark > 20:
                        print(f"   .. {dest.name}: {got/1e6:.0f}/{expect_size/1e6:.0f} MB",
                              flush=True)
                        mark = time.time()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            part.unlink(missing_ok=True)
            if attempt < tries - 1:
                time.sleep(2 ** attempt * 3)
                continue
            raise RuntimeError(f"{rel_path}: {exc}") from None

        if expect_hash and content_hash(part) != expect_hash:
            part.unlink(missing_ok=True)
            if attempt < tries - 1:
                print(f"   !! {dest.name}: hash mismatch, retrying", flush=True)
                time.sleep(3)
                continue
            raise RuntimeError(f"{rel_path}: content_hash mismatch after {tries} tries")
        part.replace(dest)
        return


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--dest", required=True, type=pathlib.Path)
    ap.add_argument("--include", nargs="*", default=None,
                    help="only these extensions, e.g. --include .mdb .csv")
    ap.add_argument("--match", default=None,
                    help="case-insensitive regex on the relative path; only "
                         "matching files are pulled. Combines with --include "
                         "(both must pass). Mirrors load_tabular.py's --match, "
                         "for selecting specific files out of an extension "
                         "class that is mostly noise -- e.g. the reference/"
                         "crosswalk CSVs and legacy R scripts sitting beside "
                         "hundreds of duplicate CSV re-exports of the same "
                         "'.mdb' tables.")
    ap.add_argument("--list-only", action="store_true")
    args = ap.parse_args()

    tok = token()
    files = walk(args.url, tok)
    if args.include:
        wanted = {e.lower() for e in args.include}
        files = [f for f in files
                 if pathlib.PurePosixPath(f["name"]).suffix.lower() in wanted]
    if args.match:
        pat = re.compile(args.match, re.I)
        files = [f for f in files if pat.search(f["_rel"])]
    files.sort(key=lambda f: f["size"])

    total = sum(f["size"] for f in files)
    print(f"{len(files)} files, {total/1e9:.2f} GB", flush=True)
    if args.list_only:
        for f in files:
            print(f"{f['size']:>14,}  {f['_rel']}")
        return 0

    manifest = []
    for i, f in enumerate(files, 1):
        rel = f["_rel"].lstrip("/")
        out = args.dest / rel
        if out.exists() and out.stat().st_size == f["size"]:
            print(f"[{i}/{len(files)}] skip {rel} (already present)", flush=True)
            manifest.append({"path": rel, "size": f["size"],
                             "content_hash": f.get("content_hash"), "ok": True})
            continue
        t0 = time.time()
        print(f"[{i}/{len(files)}] START {rel} ({f['size']/1e6:.1f} MB)", flush=True)
        try:
            download(args.url, "/" + rel, out, tok, f.get("content_hash"), f["size"])
        except RuntimeError as exc:
            print(f"[{i}/{len(files)}] FAIL  {rel}: {exc}", flush=True)
            manifest.append({"path": rel, "size": f["size"],
                             "content_hash": f.get("content_hash"),
                             "ok": False, "error": str(exc)})
            continue
        print(f"[{i}/{len(files)}] OK    {rel} in {time.time()-t0:.0f}s (hash verified)",
              flush=True)
        manifest.append({"path": rel, "size": f["size"],
                         "content_hash": f.get("content_hash"), "ok": True})

    args.dest.mkdir(parents=True, exist_ok=True)
    (args.dest / "MANIFEST.json").write_text(json.dumps(
        {"source": args.url, "pulled_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
         "files": manifest}, indent=2))
    ok = sum(1 for m in manifest if m["ok"])
    print(f"DONE: {ok} ok, {len(manifest)-ok} failed", flush=True)
    return 0 if ok == len(manifest) else 1


if __name__ == "__main__":
    sys.exit(main())
