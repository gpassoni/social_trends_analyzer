import pandas as pd
import numpy as np
from .models import Post, Comment, Subreddit 
from sqlmodel import SQLModel, Session, create_engine, select, inspect
import os
from dotenv import load_dotenv, dotenv_values
from sqlalchemy import func, case
from typing import Dict, Optional, List

load_dotenv(".env")


class RedditDBManager:
    def __init__(self, db_url=os.getenv("DATABASE_URL")):
        self.engine = create_engine(db_url)
        SQLModel.metadata.create_all(bind=self.engine)

    def reset_database(self):
        SQLModel.metadata.drop_all(bind=self.engine)
        SQLModel.metadata.create_all(bind=self.engine)
    
    def get_posts_without_comments(self) -> list[str]:
        with Session(self.engine) as session:
            results = (
                session.exec(
                    select(Post.post_id)
                    .where(~Post.comments.any())
                )
                .all()
            )
            return [post_id for post_id in results]
    
    def get_highest_priority_subreddits(self) -> list[str]:
        with Session(self.engine) as session:
            stmt = select(Subreddit).order_by(Subreddit.priority.desc())
            subreddits = session.exec(stmt).all()
            if not subreddits:
                return []
            highest_priority = subreddits[0].priority
            top_subreddits = [sub.name for sub in subreddits if sub.priority == highest_priority]
            return top_subreddits
        
    def calculate_subreddits_post_counts(self) -> dict[str, int]:
        """Return the number of posts for each subreddit as a dict."""
        with Session(self.engine) as session:
            stmt = (
                select(Post.subreddit_name, func.count(Post.post_id))
                .group_by(Post.subreddit_name)
            )
            results = session.exec(stmt).all()
            return dict(results)
    
    def calculate_subreddit_priorities(self):
        """Calculate priorities based on post counts."""
        post_counts = self.calculate_subreddits_post_counts()

        if not post_counts:
            return

        max_count = max(post_counts.values())
        threshold = max_count * 0.5

        with Session(self.engine) as session:
            for subreddit_name, post_count in post_counts.items():
                subreddit = session.get(Subreddit, subreddit_name)
                if not subreddit:
                    continue

                if post_count < 10:
                    subreddit.priority = 0
                elif post_count < threshold:
                    subreddit.priority = 1
                else:
                    subreddit.priority = 2

                session.add(subreddit)
            session.commit()
    
    def get_subreddits_priorities(self) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(Subreddit)
            subreddits = session.exec(stmt).all()
            return [{"name": sub.name, "priority": sub.priority} for sub in subreddits]
        
    def get_unlabeled_comments(self, limit: int = 512) -> list[dict[str, str]]:
        """Return a batch of comments where pred_label is None."""
        with Session(self.engine) as session:
            stmt = select(Comment).where(Comment.pred_label == None).limit(limit)
            comments = session.exec(stmt).all()
            return [{"comment_id": comment.comment_id, "body": comment.body} for comment in comments]

    def update_comments_with_sentiment(self, predictions: list[dict]):
        """
        Update Comment rows with the output from SentimentModel.
        Each dict in `predictions` must contain:
        comment_id, negative_score, neutral_score, positive_score, pred_label
        """
        with Session(self.engine) as session:
            for pred in predictions:
                comment = session.get(Comment, pred['comment_id'])
                if comment:
                    comment.negative_score = pred['negative_score']
                    comment.neutral_score = pred['neutral_score']
                    comment.positive_score = pred['positive_score']
                    comment.pred_label = pred['pred_label']
                    session.add(comment)  # opzionale, ma sicuro
            session.commit()
        
    def get_subreddit_sentiment_info(self, subreddit_name: str) -> Optional[List[Dict]]:
        """
        Return sentiment info for all comments in a subreddit in JSON-serializable format.
        
        Output: List of dicts, each dict is a comment with:
        - post_id
        - comment_id
        - pred_label
        - positive_score
        - neutral_score
        - negative_score
        - created_date (ISO 8601 string)
        """
        with Session(self.engine) as session:
            stmt = (
                select(Comment, Post.post_id)
                .join(Post, Comment.post_id == Post.post_id)
                .where(Post.subreddit_name == subreddit_name)
                .where(Comment.pred_label != None)
            )
            results = session.exec(stmt).all()

            if not results:
                return None

            data = []
            for comment, post_id in results:
                data.append({
                    "post_id": post_id,
                    "comment_id": comment.comment_id,
                    "pred_label": comment.pred_label,
                    "positive_score": comment.positive_score,
                    "neutral_score": comment.neutral_score,
                    "negative_score": comment.negative_score,
                    "created_date": comment.created_utc
                })

            return data




"""
    def get_all_sentiments_subreddit(self, subreddit: str) -> pd.DataFrame:
        with Session(self.engine) as session:
            results = (
                session.query(
                    CommentSentiment.comment_id,
                    CommentSentiment.negative_score,
                    CommentSentiment.neutral_score,
                    CommentSentiment.positive_score,
                    CommentSentiment.pred_label,
                    Comment.created_date
                )
                .join(Comment, Comment.comment_id == CommentSentiment.comment_id)
                .join(Post, Post.post_id == Comment.post_id)
                .filter(Post.subreddit == subreddit)
                .all()
            )
            df = pd.DataFrame(results, columns=[
                "comment_id", "negative_score", "neutral_score", "positive_score", "pred_label", "created_date"
            ])
            return df

    def get_hourly_sentiment(self, subreddit: str) -> pd.DataFrame:
        with Session(self.engine) as session:
            q = (
                session.query(
                    func.date_trunc("hour", Comment.created_ts).label("hour"),
                    func.avg(CommentSentiment.positive_score).label("avg_positive"),
                    func.avg(CommentSentiment.neutral_score).label("avg_neutral"),
                    func.avg(CommentSentiment.negative_score).label("avg_negative")
                )
                .join(Comment.post)
                .join(Comment.sentiment)
                .filter(Post.subreddit == subreddit)
                .group_by(func.date_trunc("hour", Comment.created_ts))
                .order_by("hour")
            )
            df = pd.read_sql(q.statement, session.bind)
            return df

    def get_daily_sentiment(self, subreddit: str) -> pd.DataFrame:
        with Session(self.engine) as session:
            q = (
                session.query(
                    func.date_trunc("day", Comment.created_date).label("day"),
                    func.avg(CommentSentiment.positive_score).label("avg_positive"),
                    func.avg(CommentSentiment.neutral_score).label("avg_neutral"),
                    func.avg(CommentSentiment.negative_score).label("avg_negative")
                )
                .join(Comment.post)
                .join(Comment.sentiment)
                .filter(Post.subreddit == subreddit)
                .group_by(func.date_trunc("day", Comment.created_date))
                .order_by("day")
            )
            df = pd.read_sql(q.statement, session.bind)
            return df

    def get_weekly_sentiment(self, subreddit: str) -> pd.DataFrame:
        with Session(self.engine) as session:
            q = (
                session.query(
                    func.date_trunc("week", Comment.created_date).label("week"),
                    func.avg(CommentSentiment.positive_score).label("avg_positive"),
                    func.avg(CommentSentiment.neutral_score).label("avg_neutral"),
                    func.avg(CommentSentiment.negative_score).label("avg_negative")
                )
                .join(Comment.post)
                .join(Comment.sentiment)
                .filter(Post.subreddit == subreddit)
                .group_by(func.date_trunc("week", Comment.created_date))
                .order_by("week")
            )
            df = pd.read_sql(q.statement, session.bind)
            return df

    def get_posts_count_for_subreddit(self, subreddit: str) -> int:
        with Session(self.engine) as session:
            count = (
                session.query(func.count(Post.post_id))
                .filter(Post.subreddit == subreddit)
                .scalar()
            )
            return count

    def get_posts_features(self):
        with Session(self.engine) as session:
            subquery = (
                session.query(
                    Comment.post_id.label("post_id"),
                    func.avg(CommentSentiment.positive_score).label("avg_positive"),
                    func.avg(CommentSentiment.neutral_score).label("avg_neutral"),
                    func.avg(CommentSentiment.negative_score).label("avg_negative"),
                    (func.sum(case((CommentSentiment.pred_label == 'positive', 1), else_=0)) * 100.0 /
                    func.count(CommentSentiment.pred_label)).label("pct_positive"),
                    (func.sum(case((CommentSentiment.pred_label == 'negative', 1), else_=0)) * 100.0 /
                    func.count(CommentSentiment.pred_label)).label("pct_negative")
                )
                .join(Comment, Comment.comment_id == CommentSentiment.comment_id)
                .group_by(Comment.post_id)
                .subquery()
            )

            results = session.query(
                Post.post_id,
                subquery.c.avg_positive,
                subquery.c.avg_neutral,
                subquery.c.avg_negative,
                subquery.c.pct_positive,
                subquery.c.pct_negative
            ).join(subquery, Post.post_id == subquery.c.post_id, isouter=True).all()

            features_list = []
            for row in results:
                features_list.append({
                    "post_id": row.post_id,
                    "fetch_type": row.fetch_type,
                    "avg_positive": row.avg_positive,
                    "avg_neutral": row.avg_neutral,
                    "avg_negative": row.avg_negative,
                    "pct_positive": row.pct_positive,
                    "pct_negative": row.pct_negative
                })
            df = pd.DataFrame(features_list)
            return df

    def get_posts_features_by_subreddit(self, subreddit: str):
        with Session(self.engine) as session:
            subquery = (
                session.query(
                    Comment.post_id.label("post_id"),
                    func.avg(CommentSentiment.positive_score).label("avg_positive"),
                    func.avg(CommentSentiment.neutral_score).label("avg_neutral"),
                    func.avg(CommentSentiment.negative_score).label("avg_negative"),
                    (func.sum(case((CommentSentiment.pred_label == 'positive', 1), else_=0)) * 100.0 /
                    func.count(CommentSentiment.pred_label)).label("pct_positive"),
                    (func.sum(case((CommentSentiment.pred_label == 'negative', 1), else_=0)) * 100.0 /
                    func.count(CommentSentiment.pred_label)).label("pct_negative"),
                    func.count(Comment.comment_id).label("num_comments")
                )
                .join(Comment, Comment.comment_id == CommentSentiment.comment_id)
                .group_by(Comment.post_id)
                .subquery()
            )

            results = session.query(
                Post.post_id,
                Post.fetch_type,
                subquery.c.avg_positive,
                subquery.c.avg_neutral,
                subquery.c.avg_negative,
                subquery.c.pct_positive,
                subquery.c.pct_negative,
                subquery.c.num_comments
            ).filter(Post.subreddit == subreddit)\
            .join(subquery, Post.post_id == subquery.c.post_id, isouter=True)\
            .all()

            features_list = []
            for row in results:
                features_list.append({
                    "post_id": row.post_id,
                    "fetch_type": row.fetch_type,
                    "avg_positive": row.avg_positive,
                    "avg_neutral": row.avg_neutral,
                    "avg_negative": row.avg_negative,
                    "pct_positive": row.pct_positive,
                    "pct_negative": row.pct_negative,
                    "num_comments": row.num_comments
                })
            df = pd.DataFrame(features_list)
            return df

    def get_all_subreddits(self) -> list[str]:
        with Session(self.engine) as session:
            results = session.query(Post.subreddit).distinct().all()
            return [sub for (sub,) in results]

    def get_all_sentiments(self) -> pd.DataFrame:
        with Session(self.engine) as session:
            results = session.query(
                CommentSentiment.comment_id,
                CommentSentiment.negative_score,
                CommentSentiment.neutral_score,
                CommentSentiment.positive_score,
                CommentSentiment.pred_label
            ).all()
            df = pd.DataFrame(results, columns=[
                "comment_id", "negative_score", "neutral_score", "positive_score", "pred_label"
            ])
            return df

    def get_comments_for_sentiment(self) -> list[dict]:
        with Session(self.engine) as session:
            results = (
                session.query(Comment.comment_id, Comment.body)
                .outerjoin(CommentSentiment)
                .filter(CommentSentiment.comment_id == None)
                .all()
            )
            return [{"comment_id": c_id, "body": body} for c_id, body in results]

    def get_all_post_ids(self) -> list[str]:
        with Session(self.engine) as session:
            results = session.query(Post.post_id).all()
            return [post_id for (post_id,) in results]

    def get_post_ids_for_subreddit(self, subreddit: str) -> list[str]:
        with Session(self.engine) as session:
            results = (
                session.query(Post.post_id)
                .filter(Post.subreddit == subreddit)
                .all()
            )
            return [post_id for (post_id,) in results]

    def load_sentiments(self, sentiments: list[dict]):
        with Session(self.engine) as session:
            try:
                session.bulk_insert_mappings(CommentSentiment, sentiments)
                session.commit()
            except IntegrityError as e:
                session.rollback()
                raise e

    def update_subreddit_status(self):
        with Session(self.engine) as session:
            post_counts = (
                session.query(
                    Post.subreddit,
                    func.count(Post.post_id).label("total_posts")
                )
                .group_by(Post.subreddit)
                .all()
            )
            post_dict = {p.subreddit: p.total_posts for p in post_counts}

            comment_counts = (
                session.query(
                    Post.subreddit,
                    func.count(Comment.comment_id).label("total_comments")
                )
                .join(Post.comments)
                .group_by(Post.subreddit)
                .all()
            )
            comment_dict = {c.subreddit: c.total_comments for c in comment_counts}

            max_posts = max(post_dict.values(), default=0)
            threshold = max_posts * 0.5

            for subreddit, total_posts in post_dict.items():
                total_comments = comment_dict.get(subreddit, 0)
                sub_status = session.get(SubredditStatus, subreddit)
                if not sub_status:
                    sub_status = SubredditStatus(subreddit=subreddit)
                    session.add(sub_status)

                sub_status.total_posts = total_posts
                sub_status.total_comments = total_comments

                if 0 < total_posts < threshold:
                    sub_status.priority = 1
                else:
                    sub_status.priority = 0

            session.commit()

    def add_new_subreddit(self, subreddit: str):
        with Session(self.engine) as session:
            existing = session.get(SubredditStatus, subreddit)
            if existing:
                print(f"Subreddit '{subreddit}' already present in the db.")
                return existing

            new_sub = SubredditStatus(
                subreddit=subreddit,
                total_posts=0,
                total_comments=0,
                priority=2
            )
            session.add(new_sub)
            session.commit()
            print(f"Subreddit '{subreddit}' added with priority 2.")
            return new_sub

    def remove_existing_subreddit(self, subreddit: str):
        with Session(self.engine) as session:
            existing = session.get(SubredditStatus, subreddit)
            if not existing:
                print(f"Subreddit '{subreddit}' not found in the db.")
                return False

            session.delete(existing)
            session.commit()
            print(f"Subreddit '{subreddit}' removed from the db.")
            return True

    def get_subreddit_status(self, subreddit: str) -> dict | None:
        with Session(self.engine) as session:
            sub_status = session.get(SubredditStatus, subreddit)
            if not sub_status:
                return None

            return {
                "subreddit": sub_status.subreddit,
                "total_posts": sub_status.total_posts,
                "total_comments": sub_status.total_comments,
                "priority": sub_status.priority,
                "last_updated": sub_status.last_updated
            }

    def get_highest_priority_subreddits(self) -> list[str]:
        with Session(self.engine) as session:
            max_priority = session.query(func.max(SubredditStatus.priority)).scalar()
            if max_priority is None:
                return []

            subreddits = (
                session.query(SubredditStatus.subreddit)
                .filter(SubredditStatus.priority == max_priority)
                .all()
            )

            return [sub[0] for sub in subreddits]

    def load_from_csv(self, csv_path: str, model_class):
        df = pd.read_csv(csv_path)
        model_columns = {
            c_attr.key
            for c_attr in inspect(model_class).mapper.column_attrs
            if c_attr.key != 'extra'
        }

        with Session(self.engine) as session:
            try:
                for _, row in df.iterrows():
                    core_data = {k: row[k] for k in df.columns if k in model_columns}
                    extra_data = {k: row[k] for k in df.columns if k not in model_columns}
                    obj = model_class(**core_data, extra=extra_data if extra_data else None)
                    session.merge(obj)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
"""
