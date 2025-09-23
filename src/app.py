import uvicorn 
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from reddit_db.models import Post, Comment, Subreddit
from reddit_db.db_manager import RedditDBManager
from sqlmodel import Session, select
from tasks.celery_app import fetch_posts, fetch_comments

app = FastAPI()
db_manager = RedditDBManager()
engine = db_manager.engine

def get_session():
    with Session(engine) as session:
        yield session

@app.post("/posts/", response_model=Post)
def create_post(post: Post, session: Session = Depends(get_session)):
    session.add(post)
    session.commit()
    session.refresh(post)
    return post

@app.get("/posts/", response_model=list[Post])
def get_posts(session: Session = Depends(get_session)):
    posts = session.exec(select(Post)).all()
    print(f"Retrieved {len(posts)} posts from the database.")
    return posts

@app.post("/comments/", response_model=Comment)
def create_comment(comment: Comment, session: Session = Depends(get_session)):
    session.add(comment)
    session.commit()
    session.refresh(comment)
    return comment

@app.post("/subreddits/", response_model=Subreddit)
def create_subreddit(subreddit: Subreddit, session: Session = Depends(get_session)):
    session.add(subreddit)
    session.commit()
    session.refresh(subreddit)
    return subreddit

@app.delete("/subreddits/{subreddit_name}", response_model=Subreddit)
def delete_subreddit(subreddit_name: str, session: Session = Depends(get_session)):
    subreddit = session.query(Subreddit).filter(Subreddit.name == subreddit_name).first()
    if not subreddit:
        raise HTTPException(status_code=404, detail="Subreddit not found")
    session.delete(subreddit)
    session.commit()
    return subreddit

@app.get("/subreddits/", response_model=list[Subreddit])
def get_subreddits(session: Session = Depends(get_session)):
    subreddits = session.exec(select(Subreddit)).all()
    return subreddits




