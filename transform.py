import os
import json
import time
import pandas as pd
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Init ---
start = time.time()
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# --- Create resilient session ---
def get_retrying_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

session = get_retrying_session()

# --- Load PRs ---
with open("data/raw.json") as f:
    raw_prs = json.load(f)

# --- Process one PR ---
def process_pr(pr):
    try:
        pr_number = pr["number"]
        title = pr["title"]
        author = pr["user"]["login"]
        merged_at = pr["merged_at"]
        commit_sha = pr["head"]["sha"]
        repo_full_name = pr["base"]["repo"]["full_name"]

        row = {
            "PR_number": pr_number,
            "Title": title,
            "Author": author,
            "Merged_At": merged_at,
            "CR_PASSED": False,
            "CHECKS_PASSED": "NO_CHECKS"
        }

        # --- Code Review ---
        reviews_url = f"https://api.github.com/repos/{repo_full_name}/pulls/{pr_number}/reviews"
        reviews_response = session.get(reviews_url, timeout=10)
        if reviews_response.ok:
            reviews = reviews_response.json()
            if any(r["state"] == "APPROVED" for r in reviews):
                row["CR_PASSED"] = True

        # --- Check-Runs ---
        check_url = f"https://api.github.com/repos/{repo_full_name}/commits/{commit_sha}/check-runs"
        check_response = session.get(check_url, timeout=10)
        if check_response.ok:
            check_data = check_response.json()
            check_runs = check_data.get("check_runs", [])
            completed = [r for r in check_runs if r["status"] == "completed"]
            if completed and all(r["conclusion"] == "success" for r in completed):
                row["CHECKS_PASSED"] = "PASSED"
            elif completed:
                row["CHECKS_PASSED"] = "FAILED"

        return row

    except Exception as e:
        print(f"⚠️ Error processing PR #{pr.get('number')}: {e}")
        return None

# --- Process PRs in batches ---
def process_all_prs(prs, batch_size=20):
    results = []
    total = len(prs)
    print(f"🚀 Starting to process {total} PRs in batches of {batch_size}...")

    for i in range(0, total, batch_size):
        batch = prs[i:i+batch_size]
        print(f"🔄 Processing batch {i//batch_size + 1} ({i+1} to {min(i+batch_size, total)})...")
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(process_pr, pr) for pr in batch]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

    return results

# --- Run ---
processed_data = process_all_prs(raw_prs, batch_size=20)
df = pd.DataFrame(processed_data)
os.makedirs("data", exist_ok=True)
df.to_parquet("data/report.parquet", index=False)

print(f"\n✅ Report saved to data/report.parquet")
print(f"📦 Total PRs processed: {len(df)}")
print(df)

end = time.time()
print(f"\n⏱️ Done in {end - start:.2f} seconds.")
