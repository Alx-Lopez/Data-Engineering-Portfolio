import time
import json
import os
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import finnhub



env_file = (os.getenv("ENV_FILE") or "").strip()
if env_file:
    dotenv_path = env_file
else:
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    local_env = os.path.join(base_dir, ".env.local")
    docker_env = os.path.join(base_dir, ".env.docker")
    example_env = os.path.join(base_dir, ".env.example")
    if os.path.exists(local_env):
        dotenv_path = local_env
    elif os.path.exists(docker_env):
        dotenv_path = docker_env
    else:
        dotenv_path = example_env
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
        event_ts = int(time.time())
        return {
            "event_id": f"{ticker}:{event_ts}",
            "source_system": "finnhub",
            "ticker": ticker,
            "current_price": Quote['c'],
            "high_price": Quote['h'],
            "low_price": Quote['l'],
            "open_price": Quote['o'],
            "previous_close_price": Quote['pc'],
            "timestamp": event_ts,
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
