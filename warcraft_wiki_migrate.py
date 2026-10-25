#!/usr/bin/env python3
import os, json, urllib.request, urllib.parse, time
OV_API_URL = os.getenv('OPENVIKING_ENDPOINT', 'http://127.0.0.1:1933')
OV_API_KEY = os.getenv('OPENVIKING_API_KEY', '')
if not OV_API_KEY:
    try:
        with open(os.path.expanduser('~/.openviking/ov.conf')) as f:
            OV_API_KEY = json.load(f).get('server',{}).get('root_api_key','')
    except: pass
OV_ACCOUNT = os.getenv('OPENVIKING_ACCOUNT','homelab')
OV_USER = os.getenv('OPENVIKING_USER','trae')
WIKI_TARGET_URI = "viking://resources/warcraft-wiki/"
STATE_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_state.json')
USER_AGENT='BruggyBot/1.0'
DELAY=1.5

def api_get(params):
    url='https://warcraft.wiki.gg/api.php?'+urllib.parse.urlencode(params,doseq=True)
    req=urllib.request.Request(url,headers={'User-Agent':USER_AGENT})
    return json.load(urllib.request.urlopen(req))

def read_resource(uri):
    headers={'X-API-Key':OV_API_KEY,'X-OpenViking-Account':OV_ACCOUNT,'X-OpenViking-User':OV_USER,'X-OpenViking-Actor-Peer':'hermes'}
    url=f"{OV_API_URL}/api/v1/content/read?uri={urllib.parse.quote(uri,'')}"
    req=urllib.request.Request(url,headers=headers)
    try:
        with urllib.request.urlopen(req,timeout=15) as r:
            return json.load(r).get('content','')
    except urllib.error.HTTPError as e:
        if e.code==404: return None
        raise

def write_resource(uri,content):
    headers={'Content-Type':'application/json','X-API-Key':OV_API_KEY,'X-OpenViking-Account':OV_ACCOUNT,'X-OpenViking-User':OV_USER,'X-OpenViking-Actor-Peer':'hermes'}
    payload={'uri':uri,'content':content,'mode':'upsert'}
    data=json.dumps(payload).encode()
    req=urllib.request.Request(f"{OV_API_URL}/api/v1/content/write",data=data,headers=headers)
    with urllib.request.urlopen(req,timeout=30) as r:
        return r.getcode()

def delete_resource(uri):
    headers={'X-API-Key':OV_API_KEY,'X-OpenViking-Account':OV_ACCOUNT,'X-OpenViking-User':OV_USER,'X-OpenViking-Actor-Peer':'hermes'}
    # OpenViking delete via write with mode delete? Use POST to /api/v1/content/delete
    try:
        data=json.dumps({'uri':uri}).encode()
        req=urllib.request.Request(f"{OV_API_URL}/api/v1/content/delete",data=data,headers={**headers,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=30) as r:
            return r.getcode()
    except Exception:
        return None

def safe(s):
    return s.replace(' ','_').replace('/','-')

def find_category_path(page, root='Forever'):
    # BFS search for page under category tree
    visited=set()
    stack=[(root,root)]
    while stack:
        cat,path=stack.pop()
        if cat in visited: continue
        visited.add(cat)
        # get members
        params={'action':'query','list':'categorymembers','cmtitle':f'Category:{cat}','cmlimit':'500','format':'json','formatversion':'2'}
        data=api_get(params)
        for m in data['query']['categorymembers']:
            ns=m.get('ns'); title=m.get('title')
            if ns==0 and title==page:
                return path
            if ns==14:
                sub=title.replace('Category:','')
                stack.append((sub, f"{path}/{sub}"))
        time.sleep(DELAY)
    return None

state=json.load(open(STATE_FILE))
moved=0
for page in state:
    # old flat uri
    old_uri=f"{WIKI_TARGET_URI}{safe(page)}.md"
    content=read_resource(old_uri)
    if not content:
        continue
    cat_path=find_category_path(page)
    if not cat_path:
        print(f"No category found for {page}, skipping")
        continue
    segments=[safe(s) for s in cat_path.split('/')]
    new_uri=f"{WIKI_TARGET_URI}/{'/'.join(segments)}/{safe(page)}.md"
    # write new
    write_resource(new_uri,content)
    # delete old
    delete_resource(old_uri)
    moved+=1
    print(f"Moved {page} -> {new_uri}")
print(f"Done, moved {moved} pages")
