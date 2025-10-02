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
    """Add a post to the database"""
    session.add(post)
    session.commit()
    session.refresh(post)
    return post

@app.post("/comments/", response_model=Comment)
def create_comment(comment: Comment, session: Session = Depends(get_session)):
    """Add a comment to the database"""
    session.add(comment)
    session.commit()
    session.refresh(comment)
    return comment

@app.post("/subreddits/", response_model=Subreddit)
def create_subreddit(subreddit: Subreddit, session: Session = Depends(get_session)):
    """Add a subreddit to the database"""
    session.add(subreddit)
    session.commit()
    session.refresh(subreddit)
    return subreddit

@app.delete("/subreddits/{subreddit_name}", response_model=Subreddit)
def delete_subreddit(subreddit_name: str, session: Session = Depends(get_session)):
    """Delete a subreddit from the database"""
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

@app.get("/posts/", response_model=list[Post])
def get_posts(session: Session = Depends(get_session)):
    posts = session.exec(select(Post)).all()
    print(f"Retrieved {len(posts)} posts from the database.")
    return posts

@app.get("/posts/ids/{subreddit_name}", response_model=list[str])
def get_posts_ids(subreddit_name: str, session: Session = Depends(get_session)):
    """Get all post ids for a given subreddit"""
    posts = session.exec(select(Post).where(Post.subreddit_name == subreddit_name)).all()
    post_ids = [post.post_id for post in posts]
    return post_ids

@app.get("/posts/count/fetch_type/{subreddit_name}", response_model=dict[str, int])
def get_posts_count_by_fetch_type(subreddit_name: str, session: Session = Depends(get_session)):
    """Get count of posts by fetch type for a given subreddit"""
    count = db_manager.get_posts_count_by_fetch_type(subreddit_name)
    if count is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return count

# ------------ Streamlit data retrieval ------------
@app.get("/data/comments/sentiment/{subreddit_name}")
def get_comments_sentiment(subreddit_name: str):
    info = db_manager.get_comments_sentiment_info(subreddit_name)
    if info is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return info

@app.get("/data/subreddits/posts_count/{subreddit_name}")
def get_subreddit_posts_count(subreddit_name: str):
    """Get the count of posts for a given subreddit"""
    count = db_manager.calculate_subreddits_post_counts()
    if count is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return {"subreddit": subreddit_name, "posts_count": count[subreddit_name]}

@app.get("/data/subreddits/subreddit_status/{subreddit_name}")
def get_subreddits_priorities(subreddit_name: str):
    """Get the priority status of the selected subreddit"""
    status = db_manager.get_subreddits_priorities(subreddit_name)
    if status is None:
        raise HTTPException(status_code=404, detail="Subreddit not found")
    return status

@app.get("/data/sentiment/hourly/{subreddit_name}")
def get_hourly_sentiment(subreddit_name: str):
    """Get sentiment data aggregated hourly for a given subreddit"""
    data = db_manager.get_hourly_sentiment(subreddit_name)
    if data is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return data
@app.get("/data/sentiment/daily/{subreddit_name}")
def get_daily_sentiment(subreddit_name: str):
    """Get sentiment data aggregated daily for a given subreddit"""
    data = db_manager.get_daily_sentiment(subreddit_name)
    if data is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return data
@app.get("/data/sentiment/weekly/{subreddit_name}")
def get_weekly_sentiment(subreddit_name: str):
    """Get sentiment data aggregated weekly for a given subreddit"""
    data = db_manager.get_weekly_sentiment(subreddit_name)
    if data is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return data

@app.get("/data/sentiment/monthly/{subreddit_name}")
def get_monthly_sentiment(subreddit_name: str):
    """Get sentiment data aggregated monthly for a given subreddit"""
    data = db_manager.get_monthly_sentiment(subreddit_name)
    if data is None:
        raise HTTPException(status_code=404, detail="Subreddit not found or no posts available")
    return data
