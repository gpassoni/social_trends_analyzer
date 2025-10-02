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
        url = "http://127.0.0.1:8000"
    ):
        load_dotenv()
        self.url = url
        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.user_agent = os.getenv("REDDIT_USER_AGENT")
        self.data_dir = os.getenv("DATA_DIR")
        self.data_dir = Path(self.data_dir)

        self.fetch_types = {"rising": self.fetch_rising_posts,
                            "hot": self.fetch_hot_posts,
                            "controversial": self.fetch_controversial_posts,
                            "top": self.fetch_top_posts,}
        
        self.post_fetched_limit = 2 # max number of posts to fetch per subreddit per run
        self.post_fetched_count = 0

        self.comments_per_post_limit = 100 # max number of comments to save per post
        self.posts_per_call_limit = 10 # number of posts to fetch per API call

        self.already_fetched_post_ids = []

        if isinstance(keyword, list):
            self.keyword = [kw.lower() for kw in keyword]
        elif isinstance(keyword, str):
            self.keyword = keyword.lower()
        else:
            self.keyword = None

        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent
        )

    def is_moderator(self, author_name: str) -> bool:
        """Check if a user is a moderator"""
        if author_name is None:
            return False
        return "moderator" in author_name.lower()

    def comment_check(self, comment) -> bool:
        """Check if a comment is valid for processing"""
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
        """Convert a PRAW comment object to a dictionary"""
        comment_dict = {
            "comment_id": str(comment.id),
            "post_id": str(comment.submission.id),
            "author": str(comment.author),
            "body": str(comment.body),
            "score": int(comment.score),
            "created_utc": int(comment.created_utc),
            "created_datetime": datetime.fromtimestamp(comment.created_utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        return comment_dict

    def post_to_dict(self, post, fetch_type: str) -> Dict[str, Any]:
        """Convert a PRAW post object to a dictionary"""
        post_dict = {
            "post_id": str(post.id).lower(),
            "title": str(post.title).lower(),
            "author": str(post.author).lower(),
            "subreddit_name": str(post.subreddit).lower(),
            "score": int(post.score),
            "created_utc": int(post.created_utc),
            "created_datetime": datetime.fromtimestamp(post.created_utc).strftime("%Y-%m-%d %H:%M:%S"),
            "fetch_type": str(fetch_type).lower()
        }
        return post_dict
    
    def add_post_to_db(self, post: Dict[str, Any]) -> int:
        """Add a post to the database via API"""
        response = requests.post(f"{self.url}/posts/", json=post)
        return response.status_code

    def add_subreddit_to_db(self, subreddit_name: str) -> int:
        """Add a subreddit to the database via API"""
        subreddit = {
            "name": subreddit_name
            }
        response = requests.post(f"{self.url}/subreddits/", json=subreddit)
        return response.status_code
    
    def add_comment_to_db(self, comment: Dict[str, Any]) -> int:
        """Add a comment to the database via API"""
        response = requests.post(f"{self.url}/comments/", json=comment)
        return response.status_code

    def fetch_new_posts(self, subreddit):
        """Fetch new posts from a subreddit"""
        posts = subreddit.new(limit=self.posts_per_call_limit)
        for post in posts:
            if post.id in self.already_fetched_post_ids:
                print(f"Post {post.id} already fetched.")
                continue
            elif self.post_fetched_count >= self.post_fetched_limit:
                print(f"Post fetch limit reached: {self.post_fetched_count}")
                break

            post_data = self.post_to_dict(post, fetch_type="new")
            self.add_post_to_db(post_data)
            self.post_fetched_count += 1
            print(f"Fetched new post: {post.id}, count per subreddit is {self.post_fetched_count}")

    def fetch_top_posts(self, subreddit):
        """Fetch top posts from a subreddit"""
        posts = subreddit.top(limit=self.posts_per_call_limit, time_filter="year")
        for post in posts:
            if post.id in self.already_fetched_post_ids:
                print(f"Post {post.id} already fetched.")
                continue
            elif self.post_fetched_count >= self.post_fetched_limit:
                print(f"Post fetch limit reached: {self.post_fetched_count}")
                break

            post_data = self.post_to_dict(post, fetch_type="top")
            self.add_post_to_db(post_data)
            self.post_fetched_count += 1
            print(f"Fetched top post: {post.id}, count per subreddit is {self.post_fetched_count}")
        
    def fetch_hot_posts(self, subreddit):
        """Fetch hot posts from a subreddit"""
        posts = subreddit.hot(limit=self.posts_per_call_limit)
        for post in posts:
            if post.id in self.already_fetched_post_ids:
                print(f"Post {post.id} already fetched.")
                continue
            elif self.post_fetched_count >= self.post_fetched_limit:
                print(f"Post fetch limit reached: {self.post_fetched_count}")
                break

            post_data = self.post_to_dict(post, fetch_type="hot")
            self.add_post_to_db(post_data)
            self.post_fetched_count += 1
            print(f"Fetched hot post: {post.id}, count per subreddit is {self.post_fetched_count}")
    
    def fetch_rising_posts(self, subreddit):
        """Fetch rising posts from a subreddit"""
        posts = subreddit.rising(limit=self.posts_per_call_limit)
        for post in posts:
            if post.id in self.already_fetched_post_ids:
                print(f"Post {post.id} already fetched.")
                continue
            elif self.post_fetched_count >= self.post_fetched_limit:
                print(f"Post fetch limit reached: {self.post_fetched_count}")
                break

            post_data = self.post_to_dict(post, fetch_type="rising")
            self.add_post_to_db(post_data)
            self.post_fetched_count += 1
            print(f"Fetched rising post: {post.id}, count per subreddit is {self.post_fetched_count}")

    def fetch_controversial_posts(self, subreddit):
        """Fetch controversial posts from a subreddit"""
        posts = subreddit.controversial(limit=self.posts_per_call_limit, time_filter="year")
        for post in posts:
            if post.id in self.already_fetched_post_ids:
                print(f"Post {post.id} already fetched.")
                continue
            elif self.post_fetched_count >= self.post_fetched_limit:
                print(f"Post fetch limit reached: {self.post_fetched_count}")
                break

            post_data = self.post_to_dict(post, fetch_type="controversial")
            self.add_post_to_db(post_data)
            self.post_fetched_count += 1
            print(f"Fetched controversial post: {post.id}, count per subreddit is {self.post_fetched_count}")

    def fetch_posts(self, subreddit_name: str):
        """Logic behind fetching posts from a subreddit"""
        add_sub_response = self.add_subreddit_to_db(subreddit_name)
        subreddit = self.reddit.subreddit(subreddit_name)
        self.already_fetched_post_ids = self.get_already_fetched_post_ids(subreddit_name)

        # Get the count of posts by fetch type and sort them ascending
        # The lowest count fetch type should be fetched first to balance the dataset 
        fetch_type_counts = self.posts_fetch_type_count(subreddit_name)
        fetch_type_counts = sorted(fetch_type_counts.items(), key=lambda x: x[1])
        fetch_type_counts = dict(fetch_type_counts)
        print(f"Fetch type counts for {subreddit_name}: {fetch_type_counts}")

        # If there is a missing fetch_type, fetch it
        if len(fetch_type_counts) < len(self.fetch_types):
            print(len(fetch_type_counts), len(self.fetch_types))
            missing_fetch_types = set(self.fetch_types.keys()) - set(fetch_type_counts.keys())
            for fetch_type in missing_fetch_types:
                if self.post_fetched_count >= self.post_fetched_limit:
                    break
                print(f"Fetching {fetch_type} posts for subreddit: {subreddit_name} (from missing fetch types)")
                self.fetch_types[fetch_type](subreddit)

        for fetch_type, count in fetch_type_counts.items():
            if self.post_fetched_count >= self.post_fetched_limit:
                break
            print(f"Fetching {fetch_type} posts for subreddit: {subreddit_name} (post_fetched_count: {self.post_fetched_count})")
            self.fetch_types[fetch_type](subreddit)

        # increase the post fetch limit if we didn't reach enough new posts this run, or decrease it if we fetched too many
        # This helps to fetch older posts if there are no new posts available, or avoid rate limits 
        if self.post_fetched_count < self.post_fetched_limit:
            self.posts_per_call_limit += 5 if self.posts_per_call_limit < 99 else 0 # cap at 100 to avoid rate limits
            print(f"Increasing posts_per_call_limit to {self.posts_per_call_limit}")
        else:
            self.posts_per_call_limit -= 1 if self.posts_per_call_limit > 10 else 0 # floor at 10 to avoid too few posts calls
            print(f"Decreasing posts_per_call_limit to {self.posts_per_call_limit}")

        self.post_fetched_count = 0

    def posts_fetch_type_count(self, subreddit_name: str) -> Dict[str, int]:
        """Get the count of posts by fetch type for a subreddit"""
        url = f"{self.url}/posts/count/fetch_type/{subreddit_name}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        return {}
    
    def get_already_fetched_post_ids(self, subreddit_name: str) -> List[str]:
        """Get already fetched post ids for a subreddit"""
        url = f"{self.url}/posts/ids/{subreddit_name}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        return []

    def extract_comments_from_post(self, post_id):
        """Extract comments from a post object and save them to the database"""
        n_comments = 0
        submission = self.reddit.submission(id=post_id)
        submission.comments.replace_more(limit=0)
        comments = list(submission.comments)
        comments = comments[:self.comments_per_post_limit]
        for comment in comments:
            if self.comment_check(comment):
                comment_data = self.comment_to_dict(comment)
                self.add_comment_to_db(comment_data)
                n_comments += 1
        print(f"Extracted {n_comments} comments from post {post_id}")


