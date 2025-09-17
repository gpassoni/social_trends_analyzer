import streamlit as st
from reddit_db.db_manager import RedditDBManager
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

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

manager = RedditDBManager()
subreddit_status = manager.get_subreddit_status(subreddit)

if subreddit_status is not None:
    priority = subreddit_status["priority"]
    if priority == 2:
        st.warning("⚠️ This subreddit is new or has very few posts. We are still collecting data. Please check back later or refresh the page.")
    elif priority == 1:
        st.info("ℹ️ This subreddit has a low number of posts. Analysis may be limited.")

data = manager.get_all_sentiments_subreddit(subreddit)

if data is None:
    st.stop()

col1, col2, col3, col4 = st.columns(4)
non_null_comments = data[data['positive_score'].notnull()]
positive_percentage = non_null_comments[non_null_comments['pred_label'] == 'positive'].shape[0] / non_null_comments.shape[0] * 100
negative_percentage = non_null_comments[non_null_comments['pred_label'] == 'negative'].shape[0] / non_null_comments.shape[0] * 100
col1.metric("Posts analyzed", manager.get_posts_count_for_subreddit(subreddit))
col2.metric("Comments analyzed", len(non_null_comments))
col3.metric("Positive comments", f"{positive_percentage:.1f}%")
col4.metric("Negative comments", f"{negative_percentage:.1f}%")
st.markdown("---")


data = manager.get_posts_features_by_subreddit(subreddit)
cols = ["avg_negative", "avg_neutral", "avg_positive"]
df_long = data[cols].melt(var_name="sentiment", value_name="probability")
plot_sentiments_distribution_by_post(df_long)



