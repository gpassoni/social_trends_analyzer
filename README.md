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
   The raw text is cleaned, transformed, and aggregated using custom Python scripts and pipelines.

3. **Backend API**  
   A **FastAPI** backend exposes endpoints to access and manage the processed data programmatically.  
   The API can be used by the Streamlit dashboard, data pipelines, or other tools.

4. **Storage**  
   Processed results are stored in a **PostgreSQL** database, providing efficient access and persistence for later analysis.

5. **Sentiment Analysis**  
   Text is analyzed using pretrained models from [Hugging Face](https://huggingface.co/).  
   Currently, the pipeline applies [`cardiffnlp/twitter-roberta-base-sentiment`](https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment), which classifies text into positive, neutral, or negative sentiment.

6. **Visualization**  
   An interactive **Streamlit** dashboard allows users to explore sentiment trends across subreddits and timeframes (hourly, daily, weekly).

7. **Deployment**  
   The entire system is packaged with **Docker**, ensuring reproducibility and easy setup across environments.

> **Note:** In some earlier versions of this project, Apache Spark was used for data processing. If you are using those versions, please refer to the README in that branch for setup instructions.

---

## Usage

### Running Locally

1. **Start PostgreSQL** (make sure the database exists and credentials match your `.env` file).  
2. **Run the backend FastAPI**:
```bash
uvicorn src.app:app --reload 
```

3. **Run the streamlit dashboard**:
```bash
streamlit run web_app/dashboard.py
```

4. **Run the pipeline (fetch and process data)**:
```bash
python perfect.py
```

### Running with Docker
1. **Build and start the stack**:
```bash
docker compose up --build
```

This will:
- Build Docker images for backend, pipeline, Streamlit, and PostgreSQL.
- Start all services:
   - PostgreSQL (postgres) – the database container.
   - Backend (backend) – FastAPI server at http://localhost:8000.
   - Pipeline (pipeline) – automatically runs perfect.py.
   - Streamlit dashboard (streamlit) – accessible at http://localhost:8501.

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





