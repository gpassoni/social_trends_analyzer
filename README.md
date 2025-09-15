# Social Trends Analyzer

Reddit hosts millions of conversations every day, where communities discuss everything from technology to personal stories.  
But what do people really feel when they talk about certain topics? Are some discussions becoming more positive, negative, or neutral over time?  

**Social Trends Analyzer** was created to explore these questions by tracking sentiment across subreddits and over time.  
Potential use cases include:  
- Monitoring sentiment around political candidates during election periods.  
- Measuring how public opinion shifts on emerging technologies (e.g., AI, crypto, renewable energy).  
- Tracking community mood in response to major events such as product launches, market changes, or global news.  

---

## System Overview

The project integrates several technologies into a single workflow:

1. **Data Collection**  
   Reddit posts and comments are fetched from selected subreddits using [PRAW](https://praw.readthedocs.io/), the Python Reddit API Wrapper.

2. **Data Processing**  
   The raw text is cleaned, transformed, and aggregated with **Apache Spark**, making the system scalable and able to handle large volumes of Reddit data.

3. **Storage**  
   Processed results are stored in a **PostgreSQL** database, providing efficient access and persistence for later analysis.

4. **Sentiment Analysis**  
   Text is analyzed using pretrained models from [Hugging Face](https://huggingface.co/).  
   Currently, the pipeline applies [`cardiffnlp/twitter-roberta-base-sentiment`](https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment), which classifies text into positive, neutral, or negative sentiment.

5. **Visualization**  
   An interactive **Streamlit** dashboard allows users to explore sentiment trends across subreddits and timeframes (hourly, daily, weekly).

6. **Deployment**  
   The entire system is packaged with **Docker**, ensuring reproducibility and easy setup across environments.

---

## Future Improvements

Planned improvements include:

- Asynchronous data fetching with PRAW for higher efficiency.
- Enhanced subreddit search with keyword-based filtering.
- Exploration of alternative sentiment analysis models.
- Additional visualization options in the Streamlit interface.
