import pandas as pd
import numpy as np
from .models import Post, Comment, Subreddit 
from sqlmodel import SQLModel, Session, create_engine, select, inspect
import os
from dotenv import load_dotenv, dotenv_values
from sqlalchemy import func, case
from typing import Dict, Optional, List
from datetime import datetime, timedelta, timezone

load_dotenv(".env")


class RedditDBManager:
    def __init__(self, db_url=os.getenv("DATABASE_URL")):
        self.engine = create_engine(db_url)
        SQLModel.metadata.create_all(bind=self.engine)

    def reset_database(self):
        """Delete all data in the database and recreate tables."""
        SQLModel.metadata.drop_all(bind=self.engine)
        SQLModel.metadata.create_all(bind=self.engine)
    
        
    def calculate_subreddits_post_counts(self) -> dict[str, int]:
        """Return the number of posts for each subreddit as a dict."""
        with Session(self.engine) as session:
            stmt = (
                select(Post.subreddit_name, func.count(Post.post_id))
                .group_by(Post.subreddit_name)
            )
            results = session.exec(stmt).all()
            return dict(results)

    ### Priority related methods ###
    def get_highest_priority_subreddits(self) -> list[str]:
        """Return a list of subreddit names with the highest priority."""
        with Session(self.engine) as session:
            stmt = select(Subreddit).order_by(Subreddit.priority.desc())
            subreddits = session.exec(stmt).all()
            if not subreddits:
                return []
            highest_priority = subreddits[0].priority
            top_subreddits = [sub.name for sub in subreddits if sub.priority == highest_priority]
            return top_subreddits

    def get_subreddits_priorities(self) -> list[dict]:
        """Return a list of dictionaries with subreddit names and their priorities."""
        with Session(self.engine) as session:
            stmt = select(Subreddit)
            subreddits = session.exec(stmt).all()
            return [{"name": sub.name, "priority": sub.priority} for sub in subreddits]
    
    def calculate_subreddit_priorities(self):
        """Calculate priorities based on post counts relative to the maximum post count.
        Priority levels:
        - 0: Less than 10 posts
        - 1: Between 10 and 50% of max posts
        - 2: More than 50% of max posts

        This ensure that, if a new subreddit is added and very few posts are in the database, it will be prioritized.
        """
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
    
    def get_unlabeled_comments(self, limit: int = 512) -> list[dict[str, str]]:
        """Return a batch of comments where pred_label is None."""
        with Session(self.engine) as session:
            stmt = select(Comment).where(Comment.pred_label == None).limit(limit)
            comments = session.exec(stmt).all()
            return [{"comment_id": comment.comment_id, "body": comment.body} for comment in comments]
    
    def get_posts_without_comments(self) -> list[str]:
        """Return a list of post_ids for posts that have no comments. Used to fetch comments for those posts."""
        with Session(self.engine) as session:
            results = (
                session.exec(
                    select(Post.post_id)
                    .where(~Post.comments.any())
                )
                .all()
            )
            return [post_id for post_id in results]

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
        
    def get_comments_sentiment_info(self, subreddit_name: str) -> Optional[List[Dict]]:
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


    def get_hourly_sentiment(self, subreddit: str) -> list[dict]:
        """
        Return hourly aggregated sentiment data for a given subreddit.
        Output: List of dicts, each dict contains:
        - hour (ISO 8601 string)
        - avg_positive
        - avg_neutral
        - avg_negative
        Data is limited to the last 4 days.
        """

        with Session(self.engine) as session:
            limit_date = datetime.now(timezone.utc) - timedelta(days=4)
            q = (
                session.query(
                    func.date_trunc("hour", Comment.created_datetime).label("hour"),
                    func.avg(Comment.positive_score).label("avg_positive"),
                    func.avg(Comment.neutral_score).label("avg_neutral"),
                    func.avg(Comment.negative_score).label("avg_negative"),
                )
                .join(Post, Comment.post_id == Post.post_id)
                .filter(Post.subreddit_name == subreddit)
                .filter(Comment.pred_label != None)
                .filter(Comment.created_datetime >= limit_date)
                .group_by(func.date_trunc("hour", Comment.created_datetime))
                .order_by("hour")
            )
            results = session.exec(q).all()
            return [
                {
                    "hour": str(r[0]),
                    "avg_positive": float(r[1]) if r[1] is not None else None,
                    "avg_neutral": float(r[2]) if r[2] is not None else None,
                    "avg_negative": float(r[3]) if r[3] is not None else None,
                }
                for r in results
            ]

    def get_daily_sentiment(self, subreddit: str) -> list[dict]:
        """
        Return daily aggregated sentiment data for a given subreddit.
        Output: List of dicts, each dict contains:
        - day (ISO 8601 string)
        - avg_positive
        - avg_neutral
        - avg_negative

        Data is limited to the last 30 days.
        """
        with Session(self.engine) as session:
            limit_date = datetime.now(timezone.utc) - timedelta(days=30)
            q = (
                session.query(
                    func.date_trunc("day", Comment.created_datetime).label("day"),
                    func.avg(Comment.positive_score).label("avg_positive"),
                    func.avg(Comment.neutral_score).label("avg_neutral"),
                    func.avg(Comment.negative_score).label("avg_negative"),
                )
                .join(Post, Comment.post_id == Post.post_id)
                .filter(Post.subreddit_name == subreddit)
                .filter(Comment.pred_label != None)
                .filter(Comment.created_datetime >= limit_date)
                .group_by(func.date_trunc("day", Comment.created_datetime))
                .order_by("day")
            )
            results = session.exec(q).all()
            return [
                {
                    "day": str(r[0].date()),
                    "avg_positive": float(r[1]) if r[1] is not None else None,
                    "avg_neutral": float(r[2]) if r[2] is not None else None,
                    "avg_negative": float(r[3]) if r[3] is not None else None,
                }
                for r in results
            ]

    def get_weekly_sentiment(self, subreddit: str) -> list[dict]:
        """
        Return weekly aggregated sentiment data for a given subreddit.
        Output: List of dicts, each dict contains:
        - week (ISO 8601 string, first day of the week)
        - avg_positive
        - avg_neutral
        - avg_negative

        Data is limited to the last 90 days.
        """
        with Session(self.engine) as session:
            limit_date = datetime.now(timezone.utc) - timedelta(days=90)
            q = (
                session.query(
                    func.date_trunc("week", Comment.created_datetime).label("week"),
                    func.avg(Comment.positive_score).label("avg_positive"),
                    func.avg(Comment.neutral_score).label("avg_neutral"),
                    func.avg(Comment.negative_score).label("avg_negative"),
                )
                .join(Post, Comment.post_id == Post.post_id)
                .filter(Post.subreddit_name == subreddit)
                .filter(Comment.pred_label != None)
                .filter(Comment.created_datetime >= limit_date)
                .group_by(func.date_trunc("week", Comment.created_datetime))
                .order_by("week")
            )
            results = session.exec(q).all()
            return [
                {
                    "week": str(r[0].date()),
                    "avg_positive": float(r[1]) if r[1] is not None else None,
                    "avg_neutral": float(r[2]) if r[2] is not None else None,
                    "avg_negative": float(r[3]) if r[3] is not None else None,
                }
                for r in results
            ]

    def get_monthly_sentiment(self, subreddit: str) -> list[dict]:
        """
        Return monthly aggregated sentiment data for a given subreddit.
        Output: List of dicts, each dict contains:

        - month (ISO 8601 string, first day of the month)
        - avg_positive
        - avg_negative
        - avg_neutral
        """
        with Session(self.engine) as session:
            # Limite: ultimi 12 mesi
            limit_date = datetime.now(timezone.utc) - timedelta(days=365)
            
            q = (
                session.query(
                    func.date_trunc("month", Comment.created_datetime).label("month"),
                    func.avg(Comment.positive_score).label("avg_positive"),
                    func.avg(Comment.neutral_score).label("avg_neutral"),
                    func.avg(Comment.negative_score).label("avg_negative"),
                )
                .join(Post, Comment.post_id == Post.post_id)
                .filter(Post.subreddit_name == subreddit)
                .filter(Comment.pred_label != None)
                .filter(Comment.created_datetime >= limit_date)
                .group_by(func.date_trunc("month", Comment.created_datetime))
                .order_by("month")
            )
            
            results = session.exec(q).all()
            
            return [
                {
                    "month": str(r[0].date()),
                    "avg_positive": float(r[1]) if r[1] is not None else None,
                    "avg_neutral": float(r[2]) if r[2] is not None else None,
                    "avg_negative": float(r[3]) if r[3] is not None else None,
                }
                for r in results
            ]

    def get_posts_count_by_fetch_type(self, subreddit: str) -> dict[str, int]:
        """Return a dict with fetch_type as keys and counts as values for a given subreddit."""
        with Session(self.engine) as session:
            stmt = (
                select(Post.fetch_type, func.count(Post.post_id))
                .where(Post.subreddit_name == subreddit)
                .group_by(Post.fetch_type)
            )
            results = session.exec(stmt).all()
            return {fetch_type: count for fetch_type, count in results}


