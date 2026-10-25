#!/usr/bin/env python3
"""
Build a content cache for the Warcraft Wiki Forever ZIM.

Reads the mwoffliner ZIM (libzim v6, Zstd-compressed clusters) and extracts the
rendered ARTICLE BODY for every article in the article namespace, then stores
{title: cleaned_body} in a JSON file that the py3.9 ingest can read without the
zstandard/py3.11 dependency.

The ZIM can only be parsed by python3.11 (it imports the compiled zstandard
extension from the build-time venv-extract), so this helper must be run with
/opt/homebrew/bin/python3.11 and PYTHONPATH pointing at that venv's
site-packages.

Freshness: the cache is rebuilt only when the ZIM file mtime changes. The ingest
reads cache["zim_mtime"] and compares to the ZIM mtime; mismatch triggers a
rebuild via this script as a subprocess.

Usage:
  PYTHONPATH=.../venv-extract/lib/python3.11/site-packages \
    /opt/homebrew/bin/python3.11 warcraft_wiki_zim_cache_build.py [--zim PATH] [--out PATH]
"""
import json
import os
import re
import struct
import sys
import lzma


def find_zim():
    for cand in (
        os.path.expanduser(
            "~/.hermes/kanban/boards/wow-forever-/workspaces/t_97049388/out/full/warcraft-wiki-forever-full.zim"),
        os.path.expanduser(
            "~/.hermes/kanban/boards/wow-forever-/workspaces/t_97049388/attachments/t_97049388/warcraft-wiki-forever-full.zim"),
        os.path.expanduser(
            "~/.hermes/kanban/boards/wow-forever-/attachments/t_97049388/warcraft-wiki-forever-full.zim"),
    ):
        if os.path.exists(cand):
            return cand
    raise FileNotFoundError("ZIM not found in known locations")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--zim", default=find_zim())
    ap.add_argument("--out",
                    default=os.path.expanduser("~/.hermes/scripts/warcraft_wiki_zim_cache.json"))
    args = ap.parse_args()

    zim_mtime = os.path.getmtime(args.zim)
    if os.path.exists(args.out):
        try:
            cached = json.load(open(args.out, encoding="utf-8"))
            if cached.get("zim_mtime") == zim_mtime and "pages" in cached:
                print("cache fresh (zim_mtime=%s); skip rebuild" % cached.get("zim_mtime"))
                return
        except Exception as exc:
            print("cache corrupt, rebuild: %s" % exc)

    raw = open(args.zim, "rb").read()
    assert struct.unpack_from("<I", raw, 0)[0] == 72173914, "bad magic"
    entry_count = struct.unpack_from("<I", raw, 24)[0]
    cluster_count = struct.unpack_from("<I", raw, 28)[0]
    path_ptr_pos = struct.unpack_from("<Q", raw, 32)[0]
    cluster_ptr_pos = struct.unpack_from("<Q", raw, 48)[0]
    mime_list_pos = struct.unpack_from("<Q", raw, 56)[0]

    # mime list
    mimes = []
    off = mime_list_pos
    while True:
        e = raw.index(b"\x00", off)
        mimes.append(raw[off:e].decode("utf-8", "replace"))
        if e == off:
            break
        off = e + 1

    def cstr(o):
        end = raw.index(b"\x00", o)
        return raw[o:end].decode("utf-8", "replace")

    # cluster decompress cache (read clusters lazily)
    cluster_cache = {}

    def decompress(i):
        if i in cluster_cache:
            return cluster_cache[i]
        cs = struct.unpack_from("<Q", raw, cluster_ptr_pos + i * 8)[0]
        ce = (struct.unpack_from("<Q", raw, cluster_ptr_pos + (i + 1) * 8)[0]
              if i + 1 < cluster_count else len(raw))
        body = raw[cs:ce]
        ct = body[0]
        if ct == 5:
            import zstandard  # py3.11 only
            body = zstandard.ZstdDecompressor().decompressobj().decompress(body[1:])
        elif ct == 4:
            body = lzma.decompress(body[1:])
        elif ct != 1:
            body = None
        cluster_cache[i] = body
        return body

    def blob_at(body, bi):
        bc = struct.unpack_from("<I", body, 0)[0] // 4
        ptrs = [struct.unpack_from("<I", body, 4 * k)[0] for k in range(bc + 1)]
        end = ptrs[bi + 1] if bi + 1 < bc else len(body)
        return body[ptrs[bi]:end]

    def extract_body(html):
        i = html.find("mw-parser-output")
        start = html.find(">", i) + 1 if i >= 0 else 0
        end = len(html)
        for foot in ("<!--/htdig_noindex-->", "creativecommons.org/licenses"):
            j = html.find(foot, start)
            if j >= 0:
                end = j
                break
        text = re.sub(r"<[^>]+>", " ", html[start:end])
        text = re.sub(r"&[a-z]+;", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    cache = {"zim": args.zim, "zim_mtime": zim_mtime, "generated": None, "pages": {}}
    n = 0
    for i in range(entry_count):
        p = struct.unpack_from("<Q", raw, path_ptr_pos + i * 8)[0]
        mime_idx = struct.unpack_from("<H", raw, p)[0]
        if mime_idx == 0xFFFF:  # redirect
            continue
        cluster = struct.unpack_from("<I", raw, p + 8)[0]
        if mime_idx != 8:  # 8 == text/html in this ZIM's mime list
            continue
        blob = struct.unpack_from("<I", raw, p + 12)[0]
        title = cstr(p + 16).replace("_", " ")
        try:
            body = decompress(cluster)
            html = blob_at(body, blob).decode("utf-8", "replace")
        except Exception:
            continue
        text = extract_body(html)
        if text:
            cache["pages"][title] = text
            n += 1
    cache["generated"] = __import__("time").strftime("%Y-%m-%dT%H:%M:%S")
    cache["count"] = n
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + ".tmp"
    json.dump(cache, open(tmp, "w", encoding="utf-8"))
    os.replace(tmp, args.out)
    print("wrote %s: %d article bodies" % (args.out, n))


if __name__ == "__main__":
    main()
