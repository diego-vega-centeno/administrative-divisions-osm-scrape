FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/data
COPY src/scrape.py ./src/

CMD ["python", "src/scrape.py"]
