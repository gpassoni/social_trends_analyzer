import time
from reddit_ingestion.reddit_ingestion import RedditIngestor
from reddit_db.db_manager import RedditDBManager
from dotenv import load_dotenv
import os

db_manager = RedditDBManager()
post_ids = db_manager.get_all_post_ids()
print(post_ids)

load_dotenv()
data_dir = os.getenv("DATA_DIR")    
print(data_dir)


default_subreddits = [
    "politics",
    "technology",
    "askspain"
]

for subreddit in default_subreddits:
    ingestor = RedditIngestor(
        post_limit=4,
        comment_limit=200,
        current_db_post_ids=post_ids,
    )

    print(f"Fetching data for subreddit: {subreddit}")

    start_time = time.time()
    ingestor.fetch(subreddit)
    elapsed = time.time() - start_time
    print(f"Completed fetching posts for subreddit: {subreddit} in {elapsed:.2f} seconds")

