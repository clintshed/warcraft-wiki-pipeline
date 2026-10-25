#!/usr/bin/env python3
"""
Warcraft Wiki HTML-fallback Forever sync.

Enumerates Category:Content added by a Forever patch via HTML fetch,
recurses into subcategories, extracts members, filters Forever version
by HTML categories, embeds with nomic-embed-text via Ollama, and upserts
to OpenViking with viking:// URI parity.

Never uses wiki API – all requests are HTML via requests (headless fallback).
Uses mcp__donsetch__web_fetch semantics: plain HTTP fetch of wiki pages.
"""
import os
import re
import json
import time
import hashlib
import urllib.parse
import urllib.request
from pathlib import Path

from bs4 import BeautifulSoup

OV_CONF = os.path.expanduser('~/.openviking/ov.conf')
STATE_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_state.json')
EMBEDDINGS_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_embeddings.jsonl')

WIKI_BASE = 'https://warcraft.wiki.gg'
FOREVER_ROOT = 'Category:Content added by a Forever patch'
WIKI_TARGET_URI = 'viking://resources/warcraft-wiki/'

NOMIC_URL = os.getenv('NOMIC_EMBED_URL', 'http://192.168.4.30:11434/api/embeddings')
NOMIC_MODEL = os.getenv('NOMIC_EMBED_MODEL', 'nomic-embed-text')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; HermesWarcraftSync/1.0)'
}

def load_ov_key():
    try:
        with open(OV_CONF) as f:
            cfg = json.load(f)
            return cfg.get('server', {}).get('root_api_key', '')
    except Exception:
        return ''

OV_API_KEY = load_ov_key()
OV_API_URL = 'http://127.0.0.1:1933'

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def safe_title(s):
    s = re.sub(r'[^A-Za-z0-9_-]+', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_')

def fetch_html(url, retries=5):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode('utf-8', errors='ignore')
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = int(e.headers.get('Retry-After', '60'))
                wait = max(wait, 30 + attempt*30)
                print(f'429 Too Many Requests for {url}, retry in {wait}s attempt {attempt+1}/{retries}')
                time.sleep(wait)
                continue
            else:
                raise
        except Exception as e:
            wait = 2 ** attempt + 5
            print(f'Fetch error {url}: {e}, retry in {wait}s')
            time.sleep(wait)
            continue
    raise RuntimeError(f'Failed to fetch {url} after {retries} attempts')

def parse_category_members(html):
    soup = BeautifulSoup(html, 'html.parser')
    # Subcategories
    subs = []
    # Pages list
    pages = []
    # Find the 'Pages in category' section
    # Wiki uses mw-category-generated
    for li in soup.select('div.mw-category-generated li'):
        a = li.find('a')
        if not a:
            continue
        title = a.get('title') or a.text.strip()
        href = a.get('href')
        # Determine if category link
        if '/wiki/Category:' in href:
            subs.append(title.replace('Category:', ''))
        else:
            pages.append(title)
    return pages, subs

def parse_page_categories(html):
    soup = BeautifulSoup(html, 'html.parser')
    cats = []
    # categories at bottom
    for a in soup.select('div#mw-normal-catlinks ul li a'):
        cats.append(a.text.strip())
    # also extract from page text categories list in footer
    # Fallback: find all category links in page
    return cats

def is_forever_page(categories, page_text):
    cats_lower = [c.lower() for c in categories]
    # Forever markers
    if any('added in patch 1.6' in c for c in cats_lower):
        return True
    if any('content added by a forever patch' in c for c in cats_lower):
        return True
    if 'forever quests' in cats_lower or 'forever' in cats_lower:
        # Heuristic: page text mentions exclusive to Forever
        if 'exclusive to world of warcraft: forever' in page_text.lower():
            return True
    # Patch 1.60.1 template presence
    if 'patch 1.60.1' in page_text.lower():
        return True
    return False

def embed_text(text):
    try:
        payload = json.dumps({'model': NOMIC_MODEL, 'prompt': text[:8000]}).encode('utf-8')
        req = urllib.request.Request(NOMIC_URL, data=payload, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.load(resp)
            vec = data.get('embedding')
            if vec:
                return vec
    except Exception as e:
        print(f'Embed failed: {e}')
    return None

def push_openviking(uri, content):
    if not OV_API_KEY:
        print('No OV API key')
        return False
    headers = {
        'X-API-Key': OV_API_KEY,
        'X-OpenViking-Account': 'homelab',
        'X-OpenViking-User': 'trae',
        'X-OpenViking-Actor-Peer': 'hermes',
        'Content-Type': 'application/json'
    }
    payload = json.dumps({'uri': uri, 'content': content, 'mode': 'create'}).encode('utf-8')
    req = urllib.request.Request(f'{OV_API_URL}/api/v1/content/write', data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status in (200, 201)
    except urllib.error.HTTPError as e:
        print(f'OV push failed {e.code}: {e.read().decode()}')
        return False

def enumerate_forever(path, root_path):
    url = f'{WIKI_BASE}/wiki/{urllib.parse.quote(path)}'
    html = fetch_html(url)
    pages, subs = parse_category_members(html)
    results = []
    for p in pages:
        results.append((p, root_path))
    for sub in subs:
        # recurse
        sub_path = f'{path}/{sub}' if path else sub
        results.extend(enumerate_forever(f'Category:{sub}', f'{root_path}/{safe_title(sub)}'))
    return results

def main():
    print('HTML-fallback Forever sync starting')
    state = load_state()
    # Start enumeration
    root_cat = FOREVER_ROOT
    # First fetch root to get subcategories
    root_url = f'{WIKI_BASE}/wiki/{urllib.parse.quote(root_cat)}'
    root_html = fetch_html(root_url)
    _, subs = parse_category_members(root_html)
    all_pages = []
    for sub in subs:
        sub_path = f'Category:{sub}'
        # Recurse one level – sub is likely Added in patch 1.60.1
        # Enumerate pages in sub
        pages, _ = parse_category_members(fetch_html(f'{WIKI_BASE}/wiki/{urllib.parse.quote(sub_path)}'))
        for p in pages:
            cat_path = f'{safe_title(root_cat)}/{safe_title(sub)}'
            all_pages.append((p, cat_path))
    print(f'Enumerated {len(all_pages)} candidate pages')
    added = updated = skipped = rejected = 0
    for page, cat_path in all_pages:
        page_url = f'{WIKI_BASE}/wiki/{urllib.parse.quote(page)}'
        try:
            html = fetch_html(page_url)
        except Exception as e:
            print(f'Fetch failed {page}: {e}')
            continue
        cats = parse_page_categories(html)
        text = BeautifulSoup(html, 'html.parser').get_text(separator=' ', strip=True)
        if not is_forever_page(cats, text):
            rejected += 1
            continue
        # State check – use last-modified header approx
        # Use simple hash of page
        page_hash = hashlib.md5(html.encode()).hexdigest()[:16]
        last = state.get(page)
        if last == page_hash:
            skipped += 1
            continue
        # Build URI
        uri_path = f'{cat_path}/{safe_title(page)}.md'
        uri = f'{WIKI_TARGET_URI}{uri_path}'
        # Embed
        vec = embed_text(text[:4000])
        embed_hash = hashlib.md5(json.dumps(vec).encode()).hexdigest()[:12] if vec else 'unavailable'
        header = f"> Source: {page_url}\n> Category: {cat_path}\n> Version: Forever\n> Embed: {NOMIC_MODEL} ({len(vec) if vec else 0}-dim, hash={embed_hash})"
        body = f'# {page}\n\n{header}\n\n{text[:20000]}'
        ok = push_openviking(uri, body)
        if ok:
            state[page] = page_hash
            if last is None:
                added += 1
            else:
                updated += 1
            print(f'Upserted {page} -> {uri}')
        else:
            print(f'Push failed {page}')
        time.sleep(1.0)
    save_state(state)
    print(f'Done. Added {added}, Updated {updated}, Skipped {skipped}, Rejected {rejected}')

if __name__ == '__main__':
    main()
