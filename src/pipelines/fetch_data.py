import time
from reddit_ingestion.reddit_ingestion import RedditIngestor
from reddit_db.db_manager import RedditDBManager
from dotenv import load_dotenv
import os

db_manager = RedditDBManager()
ingestor = RedditIngestor()

load_dotenv()
data_dir = os.getenv("DATA_DIR")
print(f"Data directory: {data_dir}")

top_subreddits = db_manager.get_highest_priority_subreddits()
print(f"Subreddit with highest priority: {top_subreddits}")
start_time = time.time()
for subreddit in top_subreddits:
    print(f"Fetching data for subreddit: {subreddit}")
    ingestor.fetch_posts(subreddit)
elapsed = time.time() - start_time
print(f"Completed fetching posts for subreddits in {elapsed:.2f} seconds")

posts_to_fetch = db_manager.get_posts_without_comments()
print(f"Number of posts without comments: {len(posts_to_fetch)}")
start_time = time.time()
for post in posts_to_fetch:
    print(f"Fetching comments for post: {post}")
    ingestor.extract_comments_from_post(post)

elapsed = time.time() - start_time
print(f"Completed fetching comments for posts in {elapsed:.2f} seconds")
elapsed = time.time() - start_time
