# Dockerfile

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk wget curl git && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src /app/src
RUN pip install --no-cache-dir -e /app/src

COPY perfect.py .

ENV PYTHONPATH="/app/src:$PYTHONPATH"

CMD ["python", "perfect.py"]
