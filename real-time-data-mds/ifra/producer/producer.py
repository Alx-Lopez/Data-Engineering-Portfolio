import time
import json
import os
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import finnhub



dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

api_key = str(os.getenv("API_KEY"))
base_url = os.getenv("BASE_URL")
default_bootstrap = "kafka:9092" if os.path.exists("/.dockerenv") else "localhost:29092"
kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", default_bootstrap)
kafka_topic = os.getenv("KAFKA_TOPIC", "stock-quotes")
finnhub_client = finnhub.Client(api_key=api_key)

if not api_key or not base_url:
    raise ValueError("API_KEY and BASE_URL environment variables must be set")
tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

producer = KafkaProducer(
    bootstrap_servers=[s.strip() for s in kafka_bootstrap_servers.split(",") if s.strip()],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
print(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")

def fetch_quote(ticker):
    Quote = finnhub_client.quote(ticker)
    if Quote:
        return {
            "ticker": ticker,
            "current_price": Quote['c'],
            "high_price": Quote['h'],
            "low_price": Quote['l'],
            "open_price": Quote['o'],
            "previous_close_price": Quote['pc'],
            "timestamp": int(time.time())
        }
    return None
    
while True:
    for ticker in tickers:
        quote = fetch_quote(ticker)
        if quote:
            producer.send(kafka_topic, value=quote)
            print(f"Producing quote for {ticker}: {quote}")
        time.sleep(1)  # To avoid hitting API rate limits
    time.sleep(10)  # Wait before fetching the next batch of quotes
