import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = "tmr232/function-graph-overview" 

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def fetch_merged_prs(repo):
    merged_prs = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{repo}/pulls"
        params = {"state": "closed", "per_page": 100, "page": page}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        filtered = [pr for pr in data if pr["merged_at"] is not None]
        if not filtered:
            break

        merged_prs.extend(filtered)
        page += 1

    return merged_prs

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    prs = fetch_merged_prs(REPO)
    with open("data/raw.json", "w") as f:
        json.dump(prs, f, indent=2)
    print(f"Saved {len(prs)} merged PRs to data/raw.json")
