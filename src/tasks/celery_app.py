import logging
from celery import Celery
from reddit_ingestion.reddit_ingestion import RedditIngestor
from reddit_db.db_manager import RedditDBManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Celery(
    'tasks',
    broker='amqp://guest:guest@localhost:5672//',
    backend='rpc://'
)

db_manager = RedditDBManager()
ingestor = RedditIngestor()

@app.task(name="tasks.celery_app.fetch_posts")
def fetch_posts(subreddit_name: str):
    logger.info("Task fetch_posts avviato")
    ingestor.fetch_new_posts(subreddit_name=subreddit_name)
    posts_to_process = db_manager.get_posts_without_comments()
    logger.info(f"Posts to process: {len(posts_to_process)}")
    for post in posts_to_process:
        fetch_comments.delay(post)
    logger.info("Task fetch_posts completato")

@app.task
def fetch_comments(post_id: str):
    logger.info(f"Task fetch_comments per post_id={post_id}")
    ingestor.extract_comments_from_post(post_id=post_id)
