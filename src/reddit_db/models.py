from sqlalchemy import (
    Column, Integer, String, Text, Float, DateTime, ForeignKey, func, Time, MetaData
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base(metadata=MetaData(schema="public"))

class Post(Base):
    __tablename__ = "posts"
    post_id = Column(String, primary_key=True)
    subreddit = Column(String)
    author = Column(String)
    title = Column(Text)
    selftext = Column(Text)
    created_utc = Column(Integer)
    created_ts = Column(DateTime)
    created_date = Column(DateTime)
    created_time = Column(Time)
    score = Column(Integer)
    author_comment_karma = Column(Integer)
    author_link_karma = Column(Integer)
    num_comments = Column(Integer)
    fetch_type = Column(String)

    extra = Column(JSONB, nullable=True)

    comments = relationship("Comment", back_populates="post")

class Comment(Base):
    __tablename__ = "comments"
    comment_id = Column(String, primary_key=True)
    post_id = Column(String, ForeignKey("posts.post_id"))
    parent_id = Column(String)
    author_comment_karma = Column(Integer)
    author_link_karma = Column(Integer)
    author = Column(String)
    body = Column(Text)
    created_utc = Column(Integer)
    created_ts = Column(DateTime)
    created_date = Column(DateTime)
    created_time = Column(Time)

    extra = Column(JSONB, nullable=True)

    post = relationship("Post", back_populates="comments")
    sentiment = relationship("CommentSentiment", back_populates="comment", uselist=False)

class CommentSentiment(Base):
    __tablename__ = "comment_sentiments"
    comment_id = Column(String, ForeignKey("comments.comment_id"), primary_key=True)
    neutral_score = Column(Float)
    positive_score = Column(Float)
    negative_score = Column(Float)
    pred_label = Column(String)

    comment = relationship("Comment", back_populates="sentiment", uselist=False)

class SubredditStatus(Base):
    __tablename__ = "subreddit_status"
    subreddit = Column(String, primary_key=True)
    total_posts = Column(Integer, default=0)
    total_comments = Column(Integer, default=0)
    last_updated = Column(DateTime, default=func.now(), onupdate=func.now())
    priority = Column(Integer, default=0)  # 0 = ok, 1 = pochi dati, 2 = inesistente / priorità massima



