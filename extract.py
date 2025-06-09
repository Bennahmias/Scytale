import os
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from urllib.parse import urlparse, parse_qs
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise RuntimeError("Please set GITHUB_TOKEN env var before running")

REPO      = "Scytale-exercise/scytale-repo3"
PER_PAGE  = 100
API_URL   = f"https://api.github.com/repos/{REPO}/pulls"
OUTPUT    = "data/raw.json"
PARALLEL  = 20 


def get_retrying_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
    })
    return session

# Find last page via Link header 

def discover_last_page(session: requests.Session) -> int:
    resp = session.get(API_URL, params={"state":"closed", "per_page":PER_PAGE, "page":1}, timeout=10)
    resp.raise_for_status()
    link = resp.headers.get("Link", "")
    # if no Link header, there’s only this one page
    if 'rel="last"' not in link:
        return 1 if resp.json() else 0
    for part in link.split(","):
        if 'rel="last"' in part:
            # extract the URL and parse the page number from it [1:-1] removes the <>
            url = part.split(";")[0].strip()[1:-1]
            return int(parse_qs(urlparse(url).query)["page"][0])
    return 1

# Fetch one page and return only merged PRs 
def fetch_page(session: requests.Session, page: int) -> List[dict]:
    resp = session.get(
        API_URL,
        params={"state":"closed", "per_page":PER_PAGE, "page":page},
        timeout=10
    )
    resp.raise_for_status()
    data = resp.json()
    return [pr for pr in data if pr.get("merged_at")]

# Fetch all the treads to one list of merged PRs
def fetch_all_merged_prs(parallel: int = PARALLEL) -> List[dict]:
    session = get_retrying_session()
    last_page = discover_last_page(session)
    if last_page == 0:
        return []

    print(f"Detected {last_page} pages of closed PRs; fetching up to {last_page * PER_PAGE} items…")

    merged: List[dict] = []
    with ThreadPoolExecutor(max_workers=parallel) as exe:
        futures = {exe.submit(fetch_page, session, pg): pg for pg in range(1, last_page+1)}
        for fut in as_completed(futures):
            pg = futures[fut]
            try:
                prs = fut.result()
                merged.extend(prs)
            except Exception as e:
                print(f"Error on page {pg}: {e}")

    return merged


if __name__ == "__main__":
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

    all_prs = fetch_all_merged_prs()
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(all_prs, f, indent=2)