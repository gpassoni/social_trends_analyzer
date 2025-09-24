from reddit_db.db_manager import RedditDBManager
from sentiment_model.sentiment_model import SentimentModel

db_manager = RedditDBManager()
model = SentimentModel()

db_batch_size = 128
model_batch_size = model.batch_size
comments_to_process = db_manager.get_unlabeled_comments(limit=512)
print(f"Found {len(comments_to_process)} comments to process...")

temp_predictions = []
for i in range(0, len(comments_to_process), model_batch_size):
    batch = comments_to_process[i:i + model_batch_size]
    preds = model.predict(batch)
    temp_predictions.extend(preds)

    if len(temp_predictions) >= db_batch_size:
        db_manager.update_comments_with_sentiment(temp_predictions)
        print(f"Loaded {len(temp_predictions)} predictions to the database...")
        temp_predictions = []

if temp_predictions:
    db_manager.update_comments_with_sentiment(temp_predictions)
    print(f"Loaded remaining {len(temp_predictions)} predictions to the database...")

print("All comments processed and loaded into the database.")
