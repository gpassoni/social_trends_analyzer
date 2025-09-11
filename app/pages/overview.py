import streamlit as st
import pandas as pd
from reddit_db.db_manager import RedditDBManager
import matplotlib.pyplot as plt
import seaborn as sns

st.title("🔍 Overview")

db_manager = RedditDBManager()

all_sentiments = db_manager.get_all_sentiments()
st.subheader("Comments Sentiment Distribution in Database")

cols = ["negative_score", "neutral_score", "positive_score"]
df_long = all_sentiments[cols].melt(var_name="Sentiment", value_name="Probability")

plt.figure(figsize=(10,6))
sns.histplot(
    data=df_long,
    x="Probability",
    hue="Sentiment",
    bins=50,
    kde=True,
    stat="density",
    common_norm=False,
    palette={"negative_score": "red", "neutral_score": "blue", "positive_score": "green"},
    alpha=0.6
)
plt.xlim(0,1)
plt.xlabel("Probability")
plt.ylabel("Density")
st.pyplot(plt)

st.write("Total comments analyzed:", len(all_sentiments))
st.write("Sentiment distribution:")
st.write(all_sentiments['pred_label'].value_counts())
st.subheader("Summary Statistics")
st.write(all_sentiments.describe())
