from prefect import flow, task
import subprocess
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()
root_dir = os.getenv("ROOT")
root_dir = Path(root_dir) 
pipelines_dir = root_dir / "src" / "pipelines"

# ----------------
# TASK DEFINITIONS
# ----------------
@task(retries=10, retry_delay_seconds=60, timeout_seconds=900)
def run_fetch_data():
    print(f"[{datetime.now()}] Running fetch_data.py")
    fetch_path = pipelines_dir / "fetch_data.py"
    subprocess.run(["python", str(fetch_path)], check=True)

@task
def run_sentiment_loading():
    print(f"[{datetime.now()}] Running sentiment_loading.py")
    sentiment_path = pipelines_dir / "sentiment_loading.py"
    subprocess.run(["python", str(sentiment_path)], check=True)

# ----------------
# FLOW DEFINITIONS
# ----------------
@flow
def main_loop():
    while True:
        start_time = time.time()
        run_fetch_data()
        run_sentiment_loading()
        print(f"Cycle completed in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    main_loop()
