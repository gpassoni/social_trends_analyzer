import streamlit as st

st.set_page_config(page_title="Reddit Sentiment Dashboard", page_icon="📊", layout="wide")

st.title("📊 Reddit Sentiment Analysis Dashboard")
st.subheader("Welcome 👋")

st.markdown("""
This dashboard allows you to:
1. Select a subreddit that has already been processed and explore its sentiment analysis.
2. Request a new subreddit, which will be fetched and analyzed in the background.
3. Explore daily, weekly, and monthly trends for Reddit posts and comments.

👉 Use the left-hand menu to navigate between pages.
""")
