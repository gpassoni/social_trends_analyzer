from prefect import flow, task
import subprocess
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()
root_dir = os.getenv("ROUTE")
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
def run_process_raw_data():
    print(f"[{datetime.now()}] Running process_raw_data.py")
    process_path = pipelines_dir / "process_raw_data.py"
    subprocess.run(["python", str(process_path)], check=True)

@task
def run_load_posts_comments_db():
    print(f"[{datetime.now()}] Running load_posts_comments_db.py")
    load_path = pipelines_dir / "load_posts_comments_db.py"
    subprocess.run(["python", str(load_path)], check=True)

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
    last_hourly_run = None

    while True:
        now = datetime.now()

        run_fetch_data()
        run_process_raw_data()
        run_load_posts_comments_db()
        #run_sentiment_loading()

        # aspetta 5 minuti prima della prossima iterazione
        time.sleep(300)

if __name__ == "__main__":
    main_loop()
