from reddit_db.db_manager import RedditDBManager
from sentiment_model.sentiment_model import SentimentModel

db_manager = RedditDBManager()
model = SentimentModel()

comments_to_process = db_manager.get_comments_for_sentiment()
print(f"Found {len(comments_to_process)} comments to process...")

predictions = model.predict(comments_to_process)
print(f"Processed {len(comments_to_process)} comments...")

db_manager.load_sentiments(predictions)
print(f"Loaded {len(predictions)} sentiment predictions into the database.")
