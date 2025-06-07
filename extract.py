import os
import requests
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = "EddieHubCommunity/awesome-github-profiles"

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# Fetch one page of closed PRs and filter merged ones
def fetch_page(repo, page):
    try:
        url = f"https://api.github.com/repos/{repo}/pulls"
        params = {"state": "closed", "per_page": 100, "page": page}
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        prs = response.json()
        return [pr for pr in prs if pr.get("merged_at") is not None]
    except Exception as e:
        print(f"⚠️ Error fetching page {page}: {e}")
        return []

# Fetch all pages in parallel
def fetch_all_merged_prs(repo, max_pages=500, batch_size=10):
    merged_prs = []
    page = 1
    while page <= max_pages:
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = {executor.submit(fetch_page, repo, p): p for p in range(page, page + batch_size)}
            batch_results = []
            for future in as_completed(futures):
                prs = future.result()
                if prs:
                    batch_results.extend(prs)
        if not batch_results:
            break
        merged_prs.extend(batch_results)
        page += batch_size
    return merged_prs


if __name__ == "__main__":
    start = time.time()
    os.makedirs("data", exist_ok=True)
    prs = fetch_all_merged_prs(REPO)
    with open("data/raw.json", "w") as f:
        json.dump(prs, f, indent=2)
    duration = time.time() - start
    print(f"✅ Saved {len(prs)} merged PRs to data/raw.json")
    print(f"⏱️ Done in {duration:.2f} seconds.")
