# db_manager.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect
from .config import DB_URL, ECHO_SQL
from .models import Base, Post, Comment, CommentSentiment
import pandas as pd
from sqlalchemy.exc import IntegrityError


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
        
    def load_from_csv(self, csv_path, model_class, chunk_size=1000):
        df = pd.read_csv(csv_path)

        model_columns = {
            c_attr.key
            for c_attr in inspect(model_class).mapper.column_attrs
            if c_attr.key != 'extra'
        }

        session = self.Session()
        try:
            objects = []
            for _, row in df.iterrows():
                core_data = {k: row[k] for k in df.columns if k in model_columns}
                extra_data = {k: row[k] for k in df.columns if k not in model_columns}

                obj = model_class(**core_data, extra=extra_data)
                objects.append(obj)

                if len(objects) >= chunk_size:
                    session.bulk_save_objects(objects)
                    session.commit()
                    objects = []

            if objects:
                session.bulk_save_objects(objects)
                session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
