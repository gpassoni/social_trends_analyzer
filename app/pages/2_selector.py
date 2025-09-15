import streamlit as st
from reddit_db.db_manager import RedditDBManager

st.set_page_config(page_title="Select Subreddit", page_icon="🗂", layout="wide")
st.write("DEBUG session_state:", dict(st.session_state))

st.title("🗂 Choose a Subreddit")

# List of available subreddits (mock for now)
manager = RedditDBManager()
available_subreddits = manager.get_all_subreddits()

st.subheader("📌 Subreddits already processed")
cols = st.columns(3)

for i, sub in enumerate(available_subreddits):
    with cols[i % 3]:
        if st.button(sub, use_container_width=True):
            st.session_state["selected_subreddit"] = sub
            st.write("DEBUG session_state:", dict(st.session_state))
            st.switch_page("pages/3_analysis.py")

st.markdown("---")
st.subheader("➕ Request a new subreddit")

new_sub = st.text_input("Enter the subreddit name (without r/):")
if st.button("Fetch and process"):
    if isinstance(new_sub, str) and new_sub.strip():
        st.info(f"Fetching and processing **r/{new_sub}**... this may take a while ⏳")
        manager.add_new_subreddit(new_sub)
        st.session_state["selected_subreddit"] = new_sub
        st.switch_page("pages/3_analysis.py")
    else:
        st.warning("Please enter a valid subreddit name!")
