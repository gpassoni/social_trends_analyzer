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

## Usage

### 1. Prerequisites

Before starting, ensure you have:

- **Docker**: [Install Docker](https://docs.docker.com/get-docker/)  
  - On **Windows/macOS**, Docker Compose is included in Docker Desktop.  
  - On **Linux**, recent Docker versions include `docker compose` as a plugin. If not, install Docker Compose separately: [Install Docker Compose](https://docs.docker.com/compose/install/).

- **Reddit Account**: You need a Reddit account to fetch data. [Sign up here](https://www.reddit.com/register/).

- **Reddit API Credentials**: To access Reddit's API, you need to obtain API credentials (Client ID, Client Secret, User Agent).  
  For detailed instructions, see the official Reddit API wiki: [https://www.reddit.com/r/reddit.com/wiki/api/](https://www.reddit.com/r/reddit.com/wiki/api/)

---

### 2. Environment Variables

Create a file called `docker.env` in the project root. Add the following variables:

```env
# PostgreSQL settings
POSTGRES_USER=analyzer
POSTGRES_PASSWORD=your_password
POSTGRES_DB=social_trends
POSTGRES_PORT=5432

# Streamlit dashboard
STREAMLIT_PORT=8501

# Reddit API credentials
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=your_user_agent
```

This file allows the containers to communicate correctly and access the Reddit API.

### Build and Start 

From the project root, run:
```bash
docker-compose up --build
```

This command will:

1. **Build the Docker images.**  
2. **Start three services**:
   - **PostgreSQL (`postgres`)** – the database container.  
   - **Pipeline (`perfect`)** – automatically runs `perfect.py`.  
   - **Streamlit dashboard (`streamlit`)** – accessible at [http://localhost:8501](http://localhost:8501).  

You will see logs for all containers in your terminal. The pipeline will fetch data, process it, analyze sentiment, and store the results in PostgreSQL.

### 4. Dashboard Access

Once the containers are running, open your browser and go to:

[http://localhost:8501](http://localhost:8501)

### 5. Initial Usage

- When you start the app for the first time, the database will be empty.  
- Use the selector to choose the subreddits you want to fetch data for, and wait for the pipeline to process them.  
- Once the initial data has been fetched, it will be saved in the database and available for future runs without needing to fetch it again.  
- Use the **Refresh** button in the top-right corner to check for updated data after a few minutes.


---

## Future Improvements

Planned improvements include:

- Asynchronous data fetching with PRAW for higher efficiency.
- Enhanced subreddit search with keyword-based filtering.
- Exploration of alternative sentiment analysis models.
- Additional visualization options in the Streamlit interface.
