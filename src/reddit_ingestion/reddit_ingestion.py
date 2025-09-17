import praw
from dotenv import load_dotenv
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from pathlib import Path

class RedditIngestor:
    def __init__(
        self,
        keyword: Union[str, List[str]] = None,
        post_limit: int = 10,
        comment_limit: Optional[int] = None,
        current_db_post_ids: Optional[List[str]] = None
    ):
        load_dotenv()
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

    def filter_post_title_by_keyword(self, title: str) -> bool:
        if not self.keyword:
            return True
        title = title.lower()
        return self.keyword in title.split()

    def fetch_comments_from_post(self, post) -> List:
        comments = post.comments.list()
        if self.comment_limit:
            comments = comments[:self.comment_limit]
        comments_dict_list = [self.comment_to_dict(c) for c in comments if self.comment_check(c)]
        return comments_dict_list

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

    def post_check(self, post) -> bool:
        if self.current_db_post_ids and post.id in self.current_db_post_ids:
            print(f"Post {post.id} already in DB, skipping.")
            return False
        self.current_db_post_ids.append(post.id) if self.current_db_post_ids is not None else None
        self.post_limit -= 1
        if self.post_limit <= 0:
            print("Post limit reached, skipping post.")
            return False
        return True

    def comment_to_dict(self, comment) -> Dict[str, Any]:
        comment_dict = {
            "comment_id": str(comment.id),
            "post_id": str(comment.submission.id),
            "parent_id": str(comment.parent_id),
            "author": str(comment.author),
            "body": str(comment.body),
            "score": int(comment.score),
            "created_utc": int(comment.created_utc)
        }
        try:
            comment_dict["author_comment_karma"] = int(comment.author.comment_karma)
            comment_dict["author_link_karma"] = int(comment.author.link_karma)
        except Exception as e:
            print(f"Error processing comment {comment.id}: {e}")
            comment_dict["author_comment_karma"] = 0
            comment_dict["author_link_karma"] = 0
        return comment_dict

    def post_to_dict(self, post, fetch_type: str) -> Dict[str, Any]:
        post_dict = {
            "post_id": str(post.id),
            "subreddit": str(post.subreddit),
            "title": str(post.title),
            "author": str(post.author),
            "score": int(post.score),
            "num_comments": int(post.num_comments),
            "created_utc": int(post.created_utc),
            "selftext": str(post.selftext),
            "fetch_type": fetch_type
        }
        try:
            post_dict["author_comment_karma"] = int(post.author.comment_karma)
            post_dict["author_link_karma"] = int(post.author.link_karma)
        except Exception as e:
            print(f"Error processing post {post.id}: {e}")
            post_dict["author_comment_karma"] = 0
            post_dict["author_link_karma"] = 0
        return post_dict

    def save_to_csv(self, post_df: pd.DataFrame, comments_df: pd.DataFrame):
        post_save_path = self.data_dir / f"raw/posts/"
        comments_save_path = self.data_dir / f"raw/comments/"
        os.makedirs(post_save_path, exist_ok=True)
        os.makedirs(comments_save_path, exist_ok=True)
        post_file = post_save_path / f"posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        comments_file = comments_save_path / f"comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        print(f"Saving posts to {post_file}")
        print(f"Saving comments to {comments_file}")
        post_df.to_csv(post_file, index=False)
        comments_df.to_csv(comments_file, index=False)

    def _fetch_posts(self, subreddit_name: str, fetch_type: str, time_filter: Optional[str] = None):
        if self.post_limit <= 0:
            print("Post limit reached, skipping fetch.")
            return pd.DataFrame(), pd.DataFrame()

        subreddit = self.reddit.subreddit(subreddit_name)
        if fetch_type == "hot":
            posts = subreddit.hot(limit=10)
        elif fetch_type == "new":
            posts = subreddit.new(limit=50)
        elif fetch_type == "top":
            posts = subreddit.top(time_filter=time_filter or "all", limit=10)
        elif fetch_type == "controversial":
            posts = subreddit.controversial(time_filter=time_filter or "all", limit=10)
        elif fetch_type == "rising":
            posts = subreddit.rising(limit=10)
        else:
            raise ValueError(f"Unknown fetch type: {fetch_type}")

        post_df = pd.DataFrame()
        comments_df = pd.DataFrame()

        for post in posts:
            if not self.post_check(post):
                continue
            if self.filter_post_title_by_keyword(post.title):
                print(f"Processing post {post.id}")
                post_data = self.post_to_dict(post, fetch_type=fetch_type)
                post_df = pd.concat([post_df, pd.DataFrame([post_data])], ignore_index=True)

                comments_data = self.fetch_comments_from_post(post)
                comments_df = pd.concat([comments_df, pd.DataFrame(comments_data)], ignore_index=True)

                print(f"Fetched post {post.id} with {len(comments_data)} comments, total posts fetched: {len(post_df)}")

        return post_df, comments_df

    def fetch_hot_posts(self, subreddit_name: str):
        return self._fetch_posts(subreddit_name, fetch_type="hot")

    def fetch_new_posts(self, subreddit_name: str):
        return self._fetch_posts(subreddit_name, fetch_type="new")

    def fetch_top_posts(self, subreddit_name: str, time_filter: str = "month"):
        return self._fetch_posts(subreddit_name, fetch_type="top", time_filter=time_filter)

    def fetch_controversial_posts(self, subreddit_name: str, time_filter: str = "week"):
        return self._fetch_posts(subreddit_name, fetch_type="controversial", time_filter=time_filter)

    def fetch(self, subreddit_name: str):
        hot_posts, hot_comments = self.fetch_hot_posts(subreddit_name)
        top_posts, top_comments = self.fetch_top_posts(subreddit_name)
        new_posts, new_comments = self.fetch_new_posts(subreddit_name)
        all_posts = pd.concat([hot_posts, top_posts, new_posts], ignore_index=True)
        all_comments = pd.concat([hot_comments, top_comments, new_comments], ignore_index=True)
        self.save_to_csv(all_posts, all_comments)
        self.save_to_csv(all_posts, all_comments)













