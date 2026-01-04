import time
import json
import os
from kafka import KafkaConsumer
import boto3
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
kafka_topic = os.getenv("KAFKA_TOPIC")
minio_endpoint_url = os.getenv("MINIO_ENDPOINT_URL")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")


if not kafka_bootstrap_servers or not kafka_topic:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC must be set")
if not minio_endpoint_url or not minio_access_key or not minio_secret_key:
    raise ValueError("MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set")
if not minio_bucket_name:
    raise ValueError("MINIO_BUCKET_NAME must be set")

s3 = boto3.client(
    "s3",
    endpoint_url=minio_endpoint_url,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key,
)


consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[s.strip() for s in kafka_bootstrap_servers.split(",") if s.strip()],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="brz-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")
for message in consumer:
    quote = message.value
    symbol = quote['ticker']
    ts = quote.get('timestamp', int(time.time()))
    file_name = f"{symbol}/{symbol}_{ts}.json"
    # print(f"Consuming quote: {quote}")
    # file_name = f"{quote['ticker']}_{quote['timestamp']}.json"
    s3.put_object(
        Bucket=minio_bucket_name,
        Key=file_name,
        Body=json.dumps(quote),
        ContentType="application/json"
    )
    print(f"Stored quote in MinIO as {file_name} in s3://{minio_bucket_name}/{file_name}")
