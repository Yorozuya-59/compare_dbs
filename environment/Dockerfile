FROM python:3.11-slim

WORKDIR /app

# build: ./environment と指定しているため、基準ディレクトリは environment になる
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --default-timeout=1000 --no-cache-dir -r requirements.txt
