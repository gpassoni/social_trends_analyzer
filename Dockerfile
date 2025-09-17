FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    build-essential \
    libpq-dev \
    wget \
    curl \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY ./src /app/src
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY perfect.py .

CMD ["python3", "./perfect.py"]
