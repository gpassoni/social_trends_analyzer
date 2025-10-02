from sqlmodel import SQLModel, Field, Session, create_engine, select, Relationship
from typing import Optional, List
from datetime import datetime

class Subreddit(SQLModel, table=True):
    name: str = Field(primary_key=True)
    priority: int = Field(default=0)

    posts: list["Post"] = Relationship(back_populates="subreddit")

class Post(SQLModel, table=True):
    post_id: str = Field(primary_key=True)
    title: str 
    author: str 
    score: int
    created_utc: int
    created_datetime: datetime
    fetch_type: str

    subreddit_name: str = Field(foreign_key="subreddit.name")
    subreddit: Optional[Subreddit] = Relationship(back_populates="posts")

    comments: list["Comment"] = Relationship(back_populates="post")

class Comment(SQLModel, table=True):
    comment_id: str = Field(primary_key=True)
    post_id: str = Field(foreign_key="post.post_id")
    author: str
    body: str
    score: int
    created_utc: int
    created_datetime: datetime

    # To be created after sentiment analysis
    negative_score: Optional[float] = None
    neutral_score: Optional[float] = None
    positive_score: Optional[float] = None
    pred_label: Optional[str] = None

    post: Optional[Post] = Relationship(back_populates="comments")
