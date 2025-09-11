# db_manager.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect
from .config import DB_URL, ECHO_SQL
from .models import Base, Post, Comment, CommentSentiment
import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert
import numpy as np


class RedditDBManager:
    def __init__(self, db_url=DB_URL, echo=ECHO_SQL):
        self.engine = create_engine(db_url, echo=echo)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def reset_database(self):
        """Drops all tables and recreates them from Base.metadata"""
        print("Dropping all tables...")
        Base.metadata.drop_all(self.engine)
        print("Creating all tables fresh...")
        Base.metadata.create_all(self.engine)
        print("Database reset complete.")

    def get_all_sentiments(self) -> pd.DataFrame:
        """
        Ritorna tutti i sentiment dal DB come DataFrame pandas.
        Colonne: comment_id, negative_score, neutral_score, positive_score, pred_label
        """
        session = self.Session()
        try:
            results = session.query(
                CommentSentiment.comment_id,
                CommentSentiment.negative_score,
                CommentSentiment.neutral_score,
                CommentSentiment.positive_score,
                CommentSentiment.pred_label
            ).all()

            df = pd.DataFrame(results, columns=[
                "comment_id", "negative_score", "neutral_score",
                "positive_score", "pred_label"
            ])
            return df
        finally:
            session.close()

    def get_comments_for_sentiment(self):
        """
        Ritorna tutti i commenti (comment_id e body) che non hanno ancora un record
        in CommentSentiment.
        """
        session = self.Session()
        try:
            results = (
                session.query(Comment.comment_id, Comment.body)
                .outerjoin(CommentSentiment)
                .filter(CommentSentiment.comment_id == None)
                .all()
            )
            return [{"comment_id": c_id, "body": body} for c_id, body in results]
        finally:
            session.close()

    def get_all_post_ids(self) -> list[str]:
        session = self.Session()
        try:
            results = session.query(Post.post_id).all()
            return [post_id for (post_id,) in results]
        finally:
            session.close()

    def load_sentiments(self, sentiments: list[dict]):
            """
            Inserisce nel DB le predizioni di sentiment.
            sentiments: lista di dizionari con chiavi
                ['comment_id', 'negative_score', 'neutral_score', 'positive_score', 'pred_label']
            """
            session = self.Session()
            try:
                # usa bulk_insert_mappings direttamente sulla lista
                session.bulk_insert_mappings(CommentSentiment, sentiments)
                session.commit()
            except IntegrityError as e:
                session.rollback()
                raise e
            finally:
                session.close()
        
    def load_from_csv(self, csv_path, model_class):
        df = pd.read_csv(csv_path)

        # colonne del modello
        model_columns = {
            c_attr.key
            for c_attr in inspect(model_class).mapper.column_attrs
            if c_attr.key != 'extra'
        }

        session = self.Session()
        try:
            for _, row in df.iterrows():
                # prendi solo le colonne che esistono nel modello
                core_data = {k: row[k] for k in df.columns if k in model_columns}

                # tutto il resto lo butti in extra
                extra_data = {k: row[k] for k in df.columns if k not in model_columns}

                obj = model_class(**core_data, extra=extra_data if extra_data else None)

                # merge = update se già esiste, insert se nuovo
                session.merge(obj)

            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()



