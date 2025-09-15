import streamlit as st
from reddit_db.db_manager import RedditDBManager
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

st.set_page_config(page_title="Sentiment Analysis", page_icon="📈", layout="wide")
st.write("DEBUG session_state:", dict(st.session_state))

if "selected_subreddit" not in st.session_state:
    st.warning("⚠️ No subreddit selected. Please go back to the previous page.")
    st.stop()

subreddit = st.session_state["selected_subreddit"]

col1, col2 = st.columns([9, 1])
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

# Quick stats
col1, col2, col3 = st.columns(3)
# count all the non null posts
non_null_posts = data[data['positive_score'].notnull()]
col1.metric("Posts analyzed", len(non_null_posts))
col2.metric("Average positive", f"{non_null_posts['positive_score'].mean():.2f}")
col3.metric("Average negative", f"{non_null_posts['negative_score'].mean():.2f}")
st.markdown("---")

# Charts (dummy for now)
st.subheader("Daily Trend")
daily_data = manager.get_daily_sentiment(subreddit)
df_melt = daily_data.melt(id_vars='day', value_vars=['avg_negative','avg_neutral','avg_positive'],
                          var_name='Sentiment', value_name='Score')

fig = px.bar(df_melt, x='day', y='Score', color='Sentiment', 
             color_discrete_map={'avg_negative':'red', 'avg_neutral':'blue', 'avg_positive':'green'})   

fig.update_layout(barmode='stack', xaxis_title='Day', yaxis_title='Average Score')
st.plotly_chart(fig)

st.subheader("Weekly Trend")
weekly_data = manager.get_weekly_sentiment(subreddit)
df_melt_weekly = weekly_data.melt(id_vars='week', value_vars=['avg_negative','avg_neutral','avg_positive'],
                                  var_name='Sentiment', value_name='Score')

fig_weekly = px.bar(df_melt_weekly, x='week', y='Score', color='Sentiment', 
                    color_discrete_map={'avg_negative':'red', 'avg_neutral':'blue', 'avg_positive':'green'})

fig_weekly.update_layout(barmode='stack', xaxis_title='Week', yaxis_title='Average Score')
st.plotly_chart(fig_weekly)
