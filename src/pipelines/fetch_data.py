import time
from reddit_ingestion.reddit_ingestion import RedditIngestor
from reddit_db.db_manager import RedditDBManager
from dotenv import load_dotenv
import os

db_manager = RedditDBManager()
db_manager.update_subreddit_status()

load_dotenv()
data_dir = os.getenv("DATA_DIR")    
print(f"Data directory: {data_dir}")

top_subreddits = db_manager.get_highest_priority_subreddits()
if not top_subreddits:
    print("Nessun subreddit con priorità trovata nel DB.")
else:
    print(f"Subreddit con priorità massima: {top_subreddits}")

for subreddit in top_subreddits:
    print(f"Fetching data for subreddit: {subreddit}")
    post_ids = db_manager.get_post_ids_for_subreddit(subreddit)
    print(f"Post IDs già presenti: {len(post_ids)}")

    ingestor = RedditIngestor(
        post_limit=5,
        comment_limit=200,
        current_db_post_ids=post_ids,
    )

    start_time = time.time()
    ingestor.fetch(subreddit)
    elapsed = time.time() - start_time
    print(f"Completed fetching posts for subreddit: {subreddit} in {elapsed:.2f} seconds")
