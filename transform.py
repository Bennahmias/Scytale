import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise RuntimeError("Please set GITHUB_TOKEN in your environment or .env file")

GITHUB_API_HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github+json"
}

def create_retrying_github_session():
    github_session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    github_session.mount("https://", adapter)
    github_session.headers.update(GITHUB_API_HEADERS)
    return github_session

github_session = create_retrying_github_session()

# Process a single PR
def analyze_pull_request(pull_request_data):
    try:
        pr_number = pull_request_data["number"]
        pr_title = pull_request_data["title"]
        pr_author = pull_request_data["user"]["login"]
        pr_merged_at = pull_request_data["merged_at"]
        pr_commit_sha = pull_request_data["head"]["sha"]
        pr_repo_full_name = pull_request_data["base"]["repo"]["full_name"]

        # default flags
        code_review_approved = False
        all_checks_passed = False

        # fetch reviews
        reviews_url = f"https://api.github.com/repos/{pr_repo_full_name}/pulls/{pr_number}/reviews"
        reviews_response = github_session.get(reviews_url, timeout=10).json()
        if any(review.get("state") == "APPROVED" for review in reviews_response):
            code_review_approved = True

        # fetch check-runs
        check_runs_url = f"https://api.github.com/repos/{pr_repo_full_name}/commits/{pr_commit_sha}/check-runs"
        check_runs_response = github_session.get(check_runs_url, timeout=10).json()
        check_runs = check_runs_response.get("check_runs", [])
        if check_runs and all(
            check.get("status") == "completed" and check.get("conclusion") == "success"
            for check in check_runs
        ):
            all_checks_passed = True

        return {
            "PR_Number":      pr_number,
            "Title":          pr_title,
            "Author":         pr_author,
            "Merged_At":      pr_merged_at,
            "Code_Review_Approved": code_review_approved,
            "All_Checks_Passed":    all_checks_passed
        }

    except Exception as error:
        print(f"Error on PR #{pull_request_data.get('number')}: {error}")
        return None

def main():

    # load raw PRs
    with open("data/raw.json", "r", encoding="utf-8") as raw_file:
        raw_pull_requests = json.load(raw_file)

    print(f"Analyzing {len(raw_pull_requests)} pull requests...")
    analyzed_pull_requests = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_pr = [executor.submit(analyze_pull_request, pr_data) for pr_data in raw_pull_requests]
        for future in as_completed(future_to_pr):
            analyzed_pr = future.result()
            if analyzed_pr:
                analyzed_pull_requests.append(analyzed_pr)

    # build DataFrame & write Parquet
    pull_requests_dataframe = pd.DataFrame(analyzed_pull_requests)
    os.makedirs("data", exist_ok=True)
    pull_requests_dataframe.to_parquet("data/report.parquet", index=False)

    print(f"Saved {len(pull_requests_dataframe)} PRs to data/report.parquet")

if __name__ == "__main__":
    main()
