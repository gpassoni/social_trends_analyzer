import praw
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from pathlib import Path
import requests

class RedditIngestor:
    def __init__(
        self,
        keyword: Union[str, List[str]] = None,
        post_limit: int = 10,
        comment_limit: Optional[int] = None,
        current_db_post_ids: Optional[List[str]] = None,
        url = "http://127.0.0.1:8000"
    ):
        load_dotenv()
        self.url = url
        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.user_agent = os.getenv("REDDIT_USER_AGENT")
        self.data_dir = os.getenv("DATA_DIR")
        self.data_dir = Path(self.data_dir)
        self.current_db_post_ids = current_db_post_ids

        if isinstance(keyword, list):
            self.keyword = [kw.lower() for kw in keyword]
        elif isinstance(keyword, str):
            self.keyword = keyword.lower()
        else:
            self.keyword = None

        self.post_limit = post_limit
        self.comment_limit = comment_limit

        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent
        )

    def is_moderator(self, author_name: str) -> bool:
        if author_name is None:
            return False
        return "moderator" in author_name.lower()

    def comment_check(self, comment) -> bool:
        if isinstance(comment, praw.models.MoreComments):
            return False
        elif not isinstance(comment, praw.models.Comment):
            return False
        elif comment.author is None:
            return False
        elif not isinstance(comment.author, praw.models.Redditor):
            return False
        elif self.is_moderator(comment.author.name):
            return False
        return True

    def comment_to_dict(self, comment) -> Dict[str, Any]:
        comment_dict = {
            "comment_id": str(comment.id),
            "post_id": str(comment.submission.id),
            "author": str(comment.author),
            "body": str(comment.body),
            "score": int(comment.score),
            "created_utc": int(comment.created_utc)
        }
        return comment_dict

    def post_to_dict(self, post, fetch_type: str) -> Dict[str, Any]:
        post_dict = {
            "post_id": str(post.id).lower(),
            "title": str(post.title).lower(),
            "author": str(post.author).lower(),
            "subreddit_name": str(post.subreddit).lower(),
            "score": int(post.score),
            "created_utc": int(post.created_utc),
            "fetch_type": str(fetch_type).lower()
        }
        return post_dict
    
    def add_post_to_db(self, post: Dict[str, Any]) -> int:
        response = requests.post(f"{self.url}/posts/", json=post)
        return response.status_code

    def add_subreddit_to_db(self, subreddit_name: str) -> int:
        subreddit = {
            "name": subreddit_name
            }
        response = requests.post(f"{self.url}/subreddits/", json=subreddit)
        return response.status_code
    
    def add_comment_to_db(self, comment: Dict[str, Any]) -> int:
        response = requests.post(f"{self.url}/comments/", json=comment)
        return response.status_code

    def fetch_new_posts(self, subreddit_name: str):
        add_sub_response = self.add_subreddit_to_db(subreddit_name)
        subreddit = self.reddit.subreddit(subreddit_name)
        posts = subreddit.new(limit=10)
        for post in posts:
            post_data = self.post_to_dict(post, fetch_type="new")
            self.add_post_to_db(post_data)

    def extract_comments_from_post(self, post_id):
        submission = self.reddit.submission(id=post_id)
        submission.comments.replace_more(limit=None)
        all_comments = submission.comments.list()

        for comment in all_comments:
            if self.comment_check(comment):
                comment_data = self.comment_to_dict(comment)
                self.add_comment_to_db(comment_data)
        
    


