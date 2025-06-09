# Scytale PR Analysis

A lightweight pipeline to extract and analyze merged pull requests (PRs) from any GitHub repository. The workflow:

Fetch all merged PRs via the GitHub API.

Verify each PR has an approved review and passed all CI checks.

Export both raw JSON data and a Parquet summary for further analysis.

---

## Features

- **Data Extraction** Retrieves every merged PR, handling pagination transparently.
- **Review & CI Validation** Flags PRs with at least one approved review and confirms all status checks succeeded.
- **Dual Output** Raw payload saved as `data/raw.json` and Aggregated report in `data/report.parquet` for efficient querying.
---

## Prerequisites

- Python 3.8+
- [pip] for dependency management
- A GitHub personal access token with `repo` scope (required for private repositories)

---

## Setup

1. **Clone the repository**

   ```sh
   git clone <https://github.com/Bennahmias/Scytale.git>
   cd Scytale
   ```

2. **Install dependencies**

   ```sh
   pip install -r requirements.txt
   ```

3. **Set up your GitHub token**

   Create a `.env` file in the project directory with this content:

   ```
   GITHUB_TOKEN=your_github_token_here
   ```

---

## Usage

### 1. Extract merged PRs

This will fetch all merged PRs from the configured repository and save them to `data/raw.json`.

```sh
python extract.py
```

### 2. Analyze PRs

This will read `data/raw.json`, check each PR for code review and CI status, and save the results to `data/report.parquet`.

```sh
python transform.py
```

---

## Viewing Results

- **View the Parquet file in Python:**

  ```python
  import pandas as pd
  df = pd.read_parquet("data/report.parquet")
  print(df.head())
  ```

- **Or use a Parquet viewer extension in VS Code.**

---

## Customization

- To change the repository being analyzed, edit the `REPO` variable in `extract.py`:

  ```python
  REPO = "owner/repo"
  ```

---

## Project Structure

```
extract.py      # Downloads merged PRs from GitHub
transform.py    # Analyzes PRs for review and CI status
data/           # Output folder for raw and processed data
requirements.txt
.env            # Your GitHub token (not committed)
```

---

## License

MIT License

---

## Author

Ben Nahmias