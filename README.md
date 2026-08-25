# Agile Analytics Pipeline

This is a data pipeline I built to extract, clean, and safely store project management data. I used the GitHub API as a source to practice handling real-world JSON data and automating the flow.

## Tools used
- Python
- Pandas
- SQLite
- Prefect

## How it works

1. Extraction: It connects to the GitHub REST API using the requests library and pulls the latest open issues.
2. Data Quality: It uses pandas to drop null IDs, remove duplicates, and select only the relevant columns.
3. Incremental Load (Upsert): Instead of appending data blindly, the script checks the database. If an issue already exists, it updates the status. If it's new, it inserts it.
4. Orchestration: The script is wrapped in Prefect to monitor execution time and track successes or failures.

## How to run

Clone this repository, install the required libraries by running pip install -r requirements.txt, and execute the script: python api_extractor.py

Note: The extraction function is fully parameterized, allowing you to easily swap the target GitHub repository URL without changing the core logic.