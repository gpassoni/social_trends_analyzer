import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from reddit_db.db_manager import RedditDBManager

st.set_page_config(page_title="Sentiment Analysis", page_icon="📈", layout="wide")

if "selected_subreddit" not in st.session_state:
    st.warning("⚠️ No subreddit selected. Please go back to the previous page.")
    st.stop()

subreddit = st.session_state["selected_subreddit"]
manager = RedditDBManager()

 
col1, col2 = st.columns([8, 2])
with col1:
    st.title(f"📈 r/{subreddit} Trends")
with col2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.session_state["__rerun"] = not st.session_state.get("__rerun", False)

 
st.markdown("---")
trend_option = st.radio(
    "Select Trend Type:",
    options=["Hourly", "Daily", "Weekly"],
    index=1,
    horizontal=True
)
st.markdown("---")


 
def plot_bar(df, date_col, title="Bar Plot of Sentiment"):
    
    df_melt = df.melt(
        id_vars=date_col,
        value_vars=['avg_positive', 'avg_neutral', 'avg_negative'],
        var_name='Sentiment',
        value_name='Score'
    )
    color_map = {'avg_positive': 'green', 'avg_neutral': 'blue', 'avg_negative': 'red'}
    fig = px.bar(
        df_melt,
        x=date_col,
        y='Score',
        color='Sentiment',
        color_discrete_map=color_map,
        barmode='stack'
    )
    fig.update_layout(
        title=title,
        xaxis_title=date_col.capitalize(),
        yaxis_title="Average Score",
        dragmode=False,
        hovermode=False,
    )
    st.plotly_chart(fig, use_container_width=True, config={'staticPlot': True})


def plot_ma(df, date_col, title="Moving Average of Sentiment", ma_window=7):
    
    df_sorted = df.sort_values(date_col).copy()
    df_sorted['avg_positive_ma'] = df_sorted['avg_positive'].rolling(ma_window, min_periods=1).mean()
    df_sorted['avg_neutral_ma'] = df_sorted['avg_neutral'].rolling(ma_window, min_periods=1).mean()
    df_sorted['avg_negative_ma'] = df_sorted['avg_negative'].rolling(ma_window, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_sorted[date_col], y=df_sorted['avg_positive_ma'],
                             mode='lines', name='Positive MA', line=dict(color='green', dash='dash')))
    fig.add_trace(go.Scatter(x=df_sorted[date_col], y=df_sorted['avg_neutral_ma'],
                             mode='lines', name='Neutral MA', line=dict(color='blue', dash='dash')))
    fig.add_trace(go.Scatter(x=df_sorted[date_col], y=df_sorted['avg_negative_ma'],
                             mode='lines', name='Negative MA', line=dict(color='red', dash='dash')))

    fig.update_layout(
        title=title,
        xaxis_title=date_col.capitalize(),
        yaxis_title="Moving Average Score",
        dragmode=False,
        hovermode=False,
    )

    st.plotly_chart(fig, use_container_width=True, config={'staticPlot': True})


 
if trend_option == "Hourly":
    st.subheader("Hourly Trends")
    df_hourly = manager.get_hourly_sentiment(subreddit)
    plot_bar(df_hourly, date_col='hour', title="Hourly Sentiment (Stacked Bars)")

    st.markdown("---")
    window_option = st.radio(
        "Select Moving Average Window:",
        options=["6-hour", "12-hour", "24-hour"],
        index=1,
        horizontal=True
    )
    ma_window = int(window_option.split("-")[0])
    plot_ma(df_hourly, date_col='hour', title=f"Hourly Sentiment ({window_option} Moving Average)", ma_window=ma_window)

elif trend_option == "Daily":
    st.subheader("Daily Trends")
    df_daily = manager.get_daily_sentiment(subreddit)
    plot_bar(df_daily, date_col='day', title="Daily Sentiment (Stacked Bars)")
    plot_ma(df_daily, date_col='day', title="Daily Sentiment (7-day Moving Average)", ma_window=7)

elif trend_option == "Weekly":
    st.subheader("Weekly Trends")
    df_weekly = manager.get_weekly_sentiment(subreddit)
    plot_bar(df_weekly, date_col='week', title="Weekly Sentiment (Stacked Bars)")
    plot_ma(df_weekly, date_col='week', title="Weekly Sentiment (4-week Moving Average)", ma_window=4)
