import json
import os
from datetime import datetime, timedelta

import boto3
from dotenv import load_dotenv
from kafka import KafkaProducer


env_file = os.getenv("ENV_FILE", "")
dotenv_path = env_file if env_file else os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

minio_endpoint_url = os.getenv("MINIO_ENDPOINT_URL")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")

replay_mode = (os.getenv("REPLAY_MODE") or "kafka").strip().lower()
replay_topic = (os.getenv("REPLAY_TOPIC") or os.getenv("KAFKA_TOPIC") or "").strip()
replay_prefix = (os.getenv("REPLAY_S3_PREFIX") or "replay").strip().strip("/")
quarantine_prefix = (os.getenv("QUARANTINE_PREFIX") or "quarantine").strip().strip("/")
replay_hours = int(os.getenv("REPLAY_HOURS", "24"))

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

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

producer = None
if replay_mode == "kafka":
    if not kafka_bootstrap_servers or not replay_topic:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS and REPLAY_TOPIC must be set for kafka replay")
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def _list_quarantine_keys():
    keys = []
    token = None
    if replay_hours <= 0:
        prefix = f"{quarantine_prefix}/"
    else:
        now = datetime.utcnow()
        prefixes = set()
        for i in range(replay_hours):
            dt = (now - timedelta(hours=i))
            prefixes.add(
                f"{quarantine_prefix}/dt={dt.strftime('%Y-%m-%d')}/hr={dt.strftime('%H')}/"
            )
        prefixes = sorted(prefixes)
    while True:
        if replay_hours <= 0:
            params = {"Bucket": minio_bucket_name, "Prefix": prefix}
            if token:
                params["ContinuationToken"] = token
            response = s3.list_objects_v2(**params)
            for obj in response.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    keys.append(obj["Key"])
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
        else:
            for prefix in prefixes:
                response = s3.list_objects_v2(Bucket=minio_bucket_name, Prefix=prefix)
                for obj in response.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        keys.append(obj["Key"])
            break
    return keys


def _replay_to_kafka(payload):
    producer.send(replay_topic, value=payload)


def _replay_to_s3(payload, key):
    dest_key = f"{replay_prefix}/{os.path.basename(key)}"
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    s3.put_object(
        Bucket=minio_bucket_name,
        Key=dest_key,
        Body=body,
        ContentType="application/json",
    )


keys = _list_quarantine_keys()
if not keys:
    print("No quarantine files found.")
    raise SystemExit(0)

for key in keys:
    body = s3.get_object(Bucket=minio_bucket_name, Key=key)["Body"].read()
    record = json.loads(body)
    payload = record.get("payload")
    if not payload:
        continue
    if replay_mode == "kafka":
        _replay_to_kafka(payload)
    else:
        _replay_to_s3(payload, key)

print(f"Replayed {len(keys)} quarantine records via {replay_mode}.")
