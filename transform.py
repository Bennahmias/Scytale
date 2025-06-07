import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
#delete
import time
start = time.time()
#delete


load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# Load the raw pull request data from JSON
with open("data/raw.json") as f:
    raw_prs = json.load(f)

# Initialize a list to hold processed PR data
processed_data = []

# Process each PR
for pr in raw_prs:
    pr_number = pr["number"]
    title = pr["title"]
    author = pr["user"]["login"]
    merged_at = pr["merged_at"]
    commit_sha = pr["head"]["sha"]
    repo_full_name = pr["base"]["repo"]["full_name"]  # example: Scytale-exercise/test

    # Prepare the row with default values for now
    row = {
        "PR_number": pr_number,
        "Title": title,
        "Author": author,
        "Merged_At": merged_at,
        "CR_PASSED": False,       
        "CHECKS_PASSED": False    
    }

    # --- Code review check  ---
    try:
        reviews_url = f"https://api.github.com/repos/{repo_full_name}/pulls/{pr_number}/reviews"
        reviews_response = requests.get(reviews_url, headers=HEADERS)
        reviews_response.raise_for_status()
        reviews = reviews_response.json()

        # Look for at least one review with state 'APPROVED'
        approved_reviews = [review for review in reviews if review["state"] == "APPROVED"]
        if approved_reviews:
            row["CR_PASSED"] = True
    except Exception as e:
        print(f"Error checking reviews for PR #{pr_number}: {e}")

    # --- Status checks passed ---
    try:
        status_url = f"https://api.github.com/repos/{repo_full_name}/commits/{commit_sha}/status"
        status_response = requests.get(status_url, headers=HEADERS)
        status_response.raise_for_status()
        status_data = status_response.json()

        #delete
        if pr_number == 184:  # or any PR number you want to inspect
            print(f"\nDEBUG: PR #{pr_number} STATUS CHECK RESPONSE:")
            print(json.dumps(status_data, indent=2))


        state = status_data.get("state")
        checks = status_data.get("statuses", [])

        if not checks:
            row["CHECKS_PASSED"] = "NO_CHECKS"
        elif state == "success":
            row["CHECKS_PASSED"] = "PASSED"
        else:
            row["CHECKS_PASSED"] = "FAILED"
    except Exception as e:
        print(f"Error checking status checks for PR #{pr_number}: {e}")

    processed_data.append(row)


df = pd.DataFrame(processed_data)

# Save to Parquet
os.makedirs("data", exist_ok=True)
df.to_parquet("data/report.parquet", index=False)

print("✅ Report saved to data/report.parquet")
sp=pd.read_parquet("data/report.parquet")
print(sp)




end = time.time()
print(f"\n⏱️ Done in {end - start:.2f} seconds.")