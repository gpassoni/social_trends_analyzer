import streamlit as st
from reddit_db.db_manager import RedditDBManager
import requests
import os 
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="Select Subreddit", page_icon="🗂", layout="wide")
api_url = os.getenv("API_URL")
subreddits_url = api_url + "subreddits/"


col1, col2 = st.columns([8, 2])
with col1:
    st.title("🗂 Manage Your Interests")
with col2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.session_state["__rerun"] = not st.session_state.get("__rerun", False)

def get_available_subreddits():
    response = requests.get(subreddits_url)
    if response.status_code == 200:
        subreddits = response.json()
        return [sub["name"] for sub in subreddits]
    
def get_n_posts_in_subreddit(subreddit_name: str) -> int:
    response = requests.get(api_url + "posts/")
    if response.status_code == 200:
        posts = response.json()
        return len(posts)
    return 0

available_subreddits = get_available_subreddits()
st.subheader("📋 Available Subreddits")
cols = st.columns(3)

if available_subreddits:
    for i, sub in enumerate(available_subreddits):
        with cols[i % 3]:
            if st.button(sub, use_container_width=True):
                st.session_state["selected_subreddit"] = sub
                st.switch_page("pages/3_analysis.py")

else:
    st.info("No subreddits available.")

st.markdown("---")
st.subheader("➕ Request a new subreddit")

def add_new_subreddit(subreddit_name: str):
    response = requests.post(subreddits_url, json={"name": subreddit_name})
    if response.status_code == 200:
        st.success(f"Subreddit '{subreddit_name}' added successfully.")
    else:
        st.error(f"Error adding subreddit: {response.text}")

def delete_subreddit(subreddit_name: str):
    response = requests.delete(subreddits_url + subreddit_name)
    if response.status_code == 200:
        st.success(f"Subreddit '{subreddit_name}' deleted successfully.")
    else:
        st.error(f"Error deleting subreddit: {response.text}")


new_sub = st.text_input("Enter the subreddit name (without r/):")
# if pressed enter, trigger the button
if st.button("Add Subreddit"):
    add_new_subreddit(new_sub)

st.markdown("---")
st.subheader("➖ Remove a subreddit")
removed_sub = st.text_input("Enter the subreddit name to remove (without r/):")
if st.button("Remove Subreddit"):
    delete_subreddit(removed_sub)


"""

if st.button("Fetch and process"):
    if isinstance(new_sub, str) and new_sub.strip():
        st.info(f"Fetching and processing **r/{new_sub}**... this may take a while ⏳")
        manager.add_new_subreddit(new_sub)
        st.session_state["selected_subreddit"] = new_sub
        st.switch_page("pages/3_analysis.py")
    else:
        st.warning("Please enter a valid subreddit name!")

for i, sub in enumerate(available_subreddits):
    with cols[i % 3]:
        if st.button(sub, use_container_width=True):
            st.session_state["selected_subreddit"] = sub
            st.switch_page("pages/3_analysis.py")

"""
