from reddit_db.db_manager import RedditDBManager
from reddit_db.models import Post, Comment
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
base_dir = os.getenv("DATA_DIR")
base_dir = Path(base_dir)
posts_dir = base_dir / "processed" / "posts"
comments_dir = base_dir / "processed" / "comments"

post_files = [f for f in posts_dir.glob("*.csv")]
comment_files = [f for f in comments_dir.glob("*.csv")]

manager = RedditDBManager()

for post_file in post_files:
    manager.load_from_csv(post_file, Post)

for comment_file in comment_files:
    manager.load_from_csv(comment_file, Comment)

raw_posts_dir = base_dir / "raw" / "posts"
raw_comments_dir = base_dir / "raw" / "comments"

for f in raw_posts_dir.glob("*"):
    print(f"Deleting {f}")
    f.unlink(missing_ok=True)

for f in raw_comments_dir.glob("*"):
    print(f"Deleting {f}")
    f.unlink(missing_ok=True)
