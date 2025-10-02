import requests
import streamlit as st
from reddit_db.db_manager import RedditDBManager
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import pandas as pd

def plot_sentiments_distribution_by_post(df):
    st.subheader("Sentiment Distribution Across Posts")

    fig = px.histogram(
        df,
        x='probability',
        color='sentiment',
        nbins=25,
    barmode='overlay',
    opacity=0.85,
    histnorm='percent',
    color_discrete_sequence=px.colors.qualitative.Set2
    )

    fig.update_traces(marker_line_width=0)
    fig.update_layout(
        xaxis_title="Sentiment Score (Probability)",
        yaxis_title="Percentage of Posts (%)",
        legend_title="Sentiment",
        xaxis=dict(fixedrange=True),
        yaxis=dict(fixedrange=True),
        dragmode=False,
        hovermode=False,
    )

    st.plotly_chart(fig, use_container_width=True, config={'staticPlot': True})

st.set_page_config(page_title="Sentiment Analysis", page_icon="📈", layout="wide")

if "selected_subreddit" not in st.session_state:
    st.warning("⚠️ No subreddit selected. Please go back to the previous page.")
    st.stop()

subreddit = st.session_state["selected_subreddit"]

col1, col2 = st.columns([8, 2])
with col1:
    st.title(f"📈 Analysis of r/{st.session_state['selected_subreddit']}")
with col2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.session_state["__rerun"] = not st.session_state.get("__rerun", False)

# TODO da rendere dinamico
subreddit_status = None
if subreddit_status is not None:
    priority = subreddit_status["priority"]
    if priority == 0:
        st.warning("⚠️ This subreddit is new or has very few posts. We are still collecting data. Please check back later or refresh the page.")
    elif priority == 1:
        st.info("ℹ️ This subreddit has a low number of posts. Analysis may be limited.")



def get_subreddit_general_info(subreddit_name):
    base_url = "http://localhost:8000"
    status_url = f"{base_url}/data/comments/sentiment/{subreddit_name}"
    response = requests.get(status_url)
    if response.status_code == 200:
        data = response.json()
    return data #list of comments with sentiment scores

def get_subreddit_posts_count(subreddit_name):
    base_url = "http://localhost:8000"
    count_url = f"{base_url}/data/subreddits/posts_count/{subreddit_name}"
    response = requests.get(count_url)
    if response.status_code == 200:
        data = response.json()
        return data # number of posts
    else:
        return 0


data = get_subreddit_general_info(subreddit_name=subreddit)
post_count = get_subreddit_posts_count(subreddit_name=subreddit)["posts_count"]
if len(data) == 0:
    st.stop()

col1, col2, col3, col4 = st.columns(4)
non_null_comments = len(data)

positive_percentage = 0
negative_percentage = 0
for comment in data:
    if comment["pred_label"] == "positive":
        positive_percentage += 1
    elif comment["pred_label"] == "negative":
        negative_percentage += 1
positive_percentage = (positive_percentage / non_null_comments) * 100 if non_null_comments > 0 else 0
negative_percentage = (negative_percentage / non_null_comments) * 100 if non_null_comments > 0 else 0
col1.metric("Posts analyzed", post_count)
col2.metric("Comments analyzed", non_null_comments)
col3.metric("Positive comments", f"{positive_percentage:.1f}%")
col4.metric("Negative comments", f"{negative_percentage:.1f}%")
st.markdown("---")

def plot_sentiments_distribution(df_melted):
    st.subheader("Sentiment Distribution of Comments")

    fig = px.histogram(
        df_melted,
        x="probability",
        color="sentiment",
        nbins=25,
        barmode="overlay",
        opacity=0.85,
        histnorm="percent",
        color_discrete_map={
            "positive_score": "green",
            "neutral_score": "blue",
            "negative_score": "red"
        }
    )

    fig.update_traces(marker_line_width=0)
    fig.update_layout(
        xaxis_title="Sentiment Score (Probability)",
        yaxis_title="Percentage of Posts (%)",
        legend_title="Sentiment",
        xaxis=dict(fixedrange=True),
        yaxis=dict(fixedrange=True),
        dragmode=False,
        hovermode=False,
    )

    st.plotly_chart(fig, use_container_width=True, config={"staticPlot": True})

df = pd.DataFrame(data)
df_melted = df.melt(
    id_vars=["post_id", "comment_id", "pred_label", "created_date"],
    value_vars=["positive_score", "neutral_score", "negative_score"],
    var_name="sentiment",
    value_name="probability"
)

plot_sentiments_distribution(df_melted)





