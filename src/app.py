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

# ------------ db operations ------------
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

# ------------ Streamlit data retrieval ------------
@app.get("/subreddits/sentiment/{subreddit_name}")
def get_subreddit_sentiment(subreddit_name: str):
    info = db_manager.get_subreddit_sentiment_info(subreddit_name)
    if info is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return info

@app.get("/subreddits/posts_count/{subreddit_name}")
def get_subreddit_posts_count(subreddit_name: str):
    count = db_manager.calculate_subreddits_post_counts()
    if count is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")

    return {"subreddit": subreddit_name, "posts_count": count[subreddit_name]}

@app.get("/subreddits/subreddit_status/{subreddit_name}")
def get_subreddits_priorities(subreddit_name: str):
    status = db_manager.get_subreddits_priorities(subreddit_name)
    if status is None:
        raise HTTPException(status_code=404, detail="Subreddit not found")
    return status
