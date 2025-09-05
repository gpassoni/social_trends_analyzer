import praw 
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd

class RedditIngestor:
    def __init__(
            self,
            subreddit_name: str,
            keyword: Union[str, List[str]] = None,
            post_limit: int = 10,
            comment_limit: Optional[int] = None
            ):
            load_dotenv()
            self.client_id = os.getenv("REDDIT_CLIENT_ID")
            self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
            self.user_agent = os.getenv("REDDIT_USER_AGENT")
            self.subreddit_name = subreddit_name

            if isinstance(keyword, list):
                self.keyword = [kw.lower() for kw in keyword]
            elif isinstance(keyword, str):
                self.keyword = keyword.lower()

            self.post_limit = post_limit
            self.comment_limit = comment_limit

            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )

    def filter_post_title_by_keyword(self, title: str) -> bool:
        if not self.keyword:
            return True

        title = title.lower()
        return self.keyword in title.split()

    def fetch_comments_from_post(self, post) -> List:
        return list(post.comments)[:self.comment_limit] if self.comment_limit else list(post.comments)

    def comment_to_dict(self, comment) -> Dict[str, Any]:
        return {
            "comment_id": str(comment.id),
            "post_id": str(comment.submission.id),
            "author": str(comment.author),
            "body": str(comment.body),
            "score": str(comment.score),
            "created_utc": datetime.utcfromtimestamp(comment.created_utc).isoformat()
        }
    def post_to_dict(self, post) -> Dict[str, Any]:
        return {
            "post_id": str(post.id),
            "title": str(post.title),
            "author": str(post.author),
            "score": str(post.score),
            "num_comments": str(post.num_comments),
            "created_utc": datetime.utcfromtimestamp(post.created_utc).isoformat(),
            "selftext": str(post.selftext)
        }

    def fetch_hot_posts(self) -> List:
        subreddit = self.reddit.subreddit(self.subreddit_name)
        hot_posts = subreddit.hot(limit=self.post_limit)

        post_df = pd.DataFrame()
        comments_df = pd.DataFrame()

        for post in hot_posts:
            if self.filter_post_title_by_keyword(post.title):
                post_data = self.post_to_dict(post)
                post_df = pd.concat([post_df, pd.DataFrame([post_data])], ignore_index=True)

                comments_data = self.fetch_comments_from_post(post)
                comments_data = [self.comment_to_dict(comment) for comment in comments_data]
                comments_df = pd.concat([comments_df, pd.DataFrame(comments_data)], ignore_index=True)

        return post_df, comments_df
        

