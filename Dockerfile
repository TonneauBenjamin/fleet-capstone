FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y openjdk-17-jre-headless procps && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
