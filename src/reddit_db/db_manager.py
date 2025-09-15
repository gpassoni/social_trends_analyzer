# db_manager.py
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import IntegrityError
import pandas as pd
import numpy as np
from .config import DB_URL, ECHO_SQL
from .models import Base, Post, Comment, CommentSentiment, SubredditStatus


class RedditDBManager:
    def __init__(self, db_url=DB_URL, echo=ECHO_SQL):
        self.engine = create_engine(db_url, echo=echo)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def reset_database(self):
        """
        Drops all tables and recreates them from Base.metadata
        """
        #print("Dropping all tables...")
        #Base.metadata.drop_all(self.engine)
        #print("Creating all tables fresh...")
        #Base.metadata.create_all(self.engine)
        #print("Database reset complete.")
        pass

    def get_daily_sentiment(self, subreddit: str) -> pd.DataFrame:
        """Returns daily average of positive, neutral, and negative scores for a given subreddit"""
        with self.Session() as session:
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
        """Returns weekly average of positive, neutral, and negative scores for a given subreddit"""
        with self.Session() as session:
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


    def get_all_sentiments_subreddit(self, subreddit: str) -> pd.DataFrame:
        """
        Returns all sentiment records for a given subreddit as a pandas DataFrame.
        Columns: comment_id, negative_score, neutral_score, positive_score, pred_label
        """
        with self.Session() as session:
            results = (
                session.query(
                    CommentSentiment.comment_id,
                    CommentSentiment.negative_score,
                    CommentSentiment.neutral_score,
                    CommentSentiment.positive_score,
                    CommentSentiment.pred_label
                )
                .join(Comment, Comment.comment_id == CommentSentiment.comment_id)
                .join(Post, Post.post_id == Comment.post_id)
                .filter(Post.subreddit == subreddit)
                .all()
            )
            df = pd.DataFrame(results, columns=[
                "comment_id", "negative_score", "neutral_score", "positive_score", "pred_label"
            ])
            return df

    def get_all_subreddits(self) -> list[str]:
        """Returns a list of all subreddits present in the database"""
        with self.Session() as session:
            results = session.query(Post.subreddit).distinct().all()
            return [sub for (sub,) in results]

    def get_all_sentiments(self) -> pd.DataFrame:
        """Returns all sentiment records from the database as a pandas DataFrame"""
        with self.Session() as session:
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
        """Returns all comments that do not yet have a sentiment record"""
        with self.Session() as session:
            results = (
                session.query(Comment.comment_id, Comment.body)
                .outerjoin(CommentSentiment)
                .filter(CommentSentiment.comment_id == None)
                .all()
            )
            return [{"comment_id": c_id, "body": body} for c_id, body in results]

    def get_all_post_ids(self) -> list[str]:
        """Returns all post IDs present in the database"""
        with self.Session() as session:
            results = session.query(Post.post_id).all()
            return [post_id for (post_id,) in results]

    def get_post_ids_for_subreddit(self, subreddit: str) -> list[str]:
        """
        Ritorna tutti i post_id presenti nel DB per un subreddit specifico.
        
        Se non ci sono post per quel subreddit, ritorna una lista vuota.
        """
        with self.Session() as session:
            results = (
                session.query(Post.post_id)
                .filter(Post.subreddit == subreddit)
                .all()
            )
            return [post_id for (post_id,) in results]

    def load_sentiments(self, sentiments: list[dict]):
        """
        Inserts sentiment predictions into the database.
        sentiments: list of dicts with keys
            ['comment_id', 'negative_score', 'neutral_score', 'positive_score', 'pred_label']
        """
        with self.Session() as session:
            try:
                session.bulk_insert_mappings(CommentSentiment, sentiments)
                session.commit()
            except IntegrityError as e:
                session.rollback()
                raise e

    def update_subreddit_status(self):
        """Aggiorna tutti i subreddit esistenti nel db e calcola la priorità."""
        with self.Session() as session:
            # 1️⃣ Conteggio post per subreddit
            post_counts = (
                session.query(
                    Post.subreddit,
                    func.count(Post.post_id).label("total_posts")
                )
                .group_by(Post.subreddit)
                .all()
            )
            post_dict = {p.subreddit: p.total_posts for p in post_counts}

            # 2️⃣ Conteggio commenti per subreddit
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

            # 3️⃣ Massimo numero di post per soglia
            max_posts = max(post_dict.values(), default=0)
            threshold = max_posts * 0.5

            # 4️⃣ Aggiornamento tabella
            for subreddit, total_posts in post_dict.items():
                total_comments = comment_dict.get(subreddit, 0)
                sub_status = session.get(SubredditStatus, subreddit)
                if not sub_status:
                    sub_status = SubredditStatus(subreddit=subreddit)
                    session.add(sub_status)

                sub_status.total_posts = total_posts
                sub_status.total_comments = total_comments

                # Calcolo priorità
                if 0 < total_posts < threshold:
                    sub_status.priority = 1
                else:
                    sub_status.priority = 0

            session.commit()

    def add_new_subreddit(self, subreddit: str):
        """
        Aggiunge un nuovo subreddit alla tabella con priorità 2.
        Se esiste già, non fa nulla.
        """
        with self.Session() as session:
            existing = session.get(SubredditStatus, subreddit)
            if existing:
                print(f"Subreddit '{subreddit}' già presente nel db.")
                return existing

            new_sub = SubredditStatus(
                subreddit=subreddit,
                total_posts=0,
                total_comments=0,
                priority=2
            )
            session.add(new_sub)
            session.commit()
            print(f"Subreddit '{subreddit}' aggiunto con priorità 2.")
            return new_sub

    def get_subreddit_status(self, subreddit: str) -> dict | None:
        with self.Session() as session:
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
        """
        Ritorna la lista dei subreddit che hanno la priorità più alta attualmente nel DB.
        """
        with self.Session() as session:
            # 1️⃣ Trova la priorità massima
            max_priority = session.query(func.max(SubredditStatus.priority)).scalar()
            if max_priority is None:
                return []

            # 2️⃣ Prendi solo i subreddit con priorità massima
            subreddits = (
                session.query(SubredditStatus.subreddit)
                .filter(SubredditStatus.priority == max_priority)
                .all()
            )

            # 3️⃣ Ritorna solo i nomi come lista di stringhe
            return [sub[0] for sub in subreddits]

    def load_from_csv(self, csv_path: str, model_class):
        """
        Loads data from a CSV file into the database.
        Non-model columns are stored in the `extra` JSONB field.
        """
        df = pd.read_csv(csv_path)
        model_columns = {
            c_attr.key
            for c_attr in inspect(model_class).mapper.column_attrs
            if c_attr.key != 'extra'
        }

        with self.Session() as session:
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
