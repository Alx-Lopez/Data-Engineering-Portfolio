import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import jsonschema
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata


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

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
kafka_topic = os.getenv("KAFKA_TOPIC")
minio_endpoint_url = os.getenv("MINIO_ENDPOINT_URL")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("S3_BUCKET") or os.getenv("MINIO_BUCKET_NAME")
use_minio = (os.getenv("USE_MINIO") or "").strip().lower() == "true"
aws_region = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "").strip() or None
aws_profile = (os.getenv("AWS_PROFILE") or "").strip() or None

batch_size = int(os.getenv("BATCH_SIZE", "500"))  # records per flush target
max_batch_seconds = int(os.getenv("MAX_BATCH_SECONDS", "30"))
max_batch_bytes = int(os.getenv("MAX_BATCH_BYTES", "5242880"))
compression = os.getenv("PARQUET_COMPRESSION", "snappy")
base_prefix = os.getenv("S3_BASE_PREFIX", "").strip().strip("/")
offset_bucket_size = int(os.getenv("OFFSET_BUCKET_SIZE", "500"))  # deterministic offset window
max_bucket_seconds = int(os.getenv("MAX_BUCKET_SECONDS", "0"))  # force flush for slow buckets
schema_validation_enabled = os.getenv("SCHEMA_VALIDATION_ENABLED", "true").lower() == "true"
schema_path = os.getenv(
    "SCHEMA_FILE",
    os.path.join(os.path.dirname(__file__), "schema", "stock_quote.schema.json"),
)
if not os.path.isabs(schema_path):
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    schema_path = os.path.abspath(os.path.join(repo_root, schema_path))
dlq_prefix = os.getenv("DLQ_PREFIX", "dlq").strip().strip("/")
quarantine_prefix = os.getenv("QUARANTINE_PREFIX", "quarantine").strip().strip("/")
kafka_dlq_topic = (os.getenv("KAFKA_DLQ_TOPIC") or "").strip()

if not kafka_bootstrap_servers or not kafka_topic:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC must be set")
if use_minio and (not minio_endpoint_url or not minio_access_key or not minio_secret_key):
    raise ValueError("USE_MINIO=true requires MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY")
if not bucket_name:
    raise ValueError("S3_BUCKET (or MINIO_BUCKET_NAME) must be set")

if not aws_profile:
    os.environ.pop("AWS_PROFILE", None)
session = boto3.Session(profile_name=aws_profile or None, region_name=aws_region or None)

if use_minio:
    s3 = session.client(
        "s3",
        endpoint_url=minio_endpoint_url,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
    )
else:
    s3 = session.client("s3")

schema_validator = None
if schema_validation_enabled:
    with open(schema_path, "r", encoding="utf-8") as schema_file:
        schema_validator = jsonschema.Draft7Validator(json.load(schema_file))


def _utc_partition(ts_seconds):
    # Partition by event time (UTC) to build stable directory layout.
    dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def _object_key(topic, partition, dt, hr, bucket_start, bucket_end):
    # Deterministic filename based on Kafka offsets for idempotent replays.
    prefix = f"{base_prefix}/" if base_prefix else ""
    return (
        f"{prefix}topic={topic}/dt={dt}/hr={hr}/partition={partition}/"
        f"start={bucket_start}_end={bucket_end}.parquet"
    )


def _object_exists(key):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _flush_batch(key, batch):
    if not batch["records"]:
        return

    topic, partition, dt, hr, bucket_start, bucket_end = key
    max_offset = batch["max_offset"]
    object_key = _object_key(topic, partition, dt, hr, bucket_start, bucket_end)

    if _object_exists(object_key):
        # Offset commit only if the deterministic file already exists.
        consumer.commit(
            {TopicPartition(topic, partition): OffsetAndMetadata(max_offset + 1, "", -1)}
        )
        batch["records"].clear()
        batch["bytes"] = 0
        batch["max_offset"] = None
        batch["first_seen"] = None
        return

    table = pa.Table.from_pylist(batch["records"])
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression=compression)
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=buffer.read(),
        ContentType="application/x-parquet",
    )

    # Commit after successful write so replays remain idempotent.
    consumer.commit(
        {TopicPartition(topic, partition): OffsetAndMetadata(max_offset + 1, "", -1)}
    )

    batch["records"].clear()
    batch["bytes"] = 0
    batch["max_offset"] = None
    batch["first_seen"] = None


consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=[s.strip() for s in kafka_bootstrap_servers.split(",") if s.strip()],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="brz-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")

batches = defaultdict(lambda: {"records": [], "bytes": 0, "max_offset": None, "first_seen": None})

dlq_producer = None
if kafka_dlq_topic:
    dlq_producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in kafka_bootstrap_servers.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def _flush_all():
    # Best-effort flush on shutdown to avoid dropping buffered records.
    for key, batch in list(batches.items()):
        if batch["records"]:
            _flush_batch(key, batch)


def _send_to_dlq(topic, partition, offset, event_ts, payload, reason):
    dt, hr = _utc_partition(event_ts)
    record = {"error": reason, "payload": payload, "event_ts": int(event_ts)}
    if dlq_prefix:
        object_key = (
            f"{dlq_prefix}/topic={topic}/dt={dt}/hr={hr}/"
            f"partition={partition}/offset={offset}.json"
        )
        body = json.dumps(record, separators=(",", ":")).encode("utf-8")
        s3.put_object(
        Bucket=bucket_name,
            Key=object_key,
            Body=body,
            ContentType="application/json",
        )
    if quarantine_prefix:
        object_key = (
            f"{quarantine_prefix}/topic={topic}/dt={dt}/hr={hr}/"
            f"partition={partition}/offset={offset}.json"
        )
        body = json.dumps(record, separators=(",", ":")).encode("utf-8")
        s3.put_object(
        Bucket=bucket_name,
            Key=object_key,
            Body=body,
            ContentType="application/json",
        )
    if dlq_producer:
        dlq_producer.send(kafka_dlq_topic, value=record)


try:
    while True:
        now = time.time()
        records = consumer.poll(timeout_ms=1000)
        for tp, messages in records.items():
            for message in messages:
                event_ts = message.timestamp / 1000 if message.timestamp else now
                dt, hr = _utc_partition(event_ts)
                bucket_start = (message.offset // offset_bucket_size) * offset_bucket_size
                bucket_end = bucket_start + offset_bucket_size - 1
                key = (message.topic, message.partition, dt, hr, bucket_start, bucket_end)
                batch = batches[key]

                payload = message.value
                if schema_validator:
                    errors = sorted(schema_validator.iter_errors(payload), key=str)
                    if errors:
                        _send_to_dlq(
                            message.topic,
                            message.partition,
                            message.offset,
                            event_ts,
                            payload,
                            errors[0].message,
                        )
                        continue
                record = dict(payload)
                record["_kafka_partition"] = message.partition
                record["_kafka_offset"] = message.offset
                record["_kafka_timestamp"] = int(event_ts)

                batch["records"].append(record)
                batch["bytes"] += len(json.dumps(payload))
                batch["max_offset"] = message.offset if batch["max_offset"] is None else max(
                    batch["max_offset"], message.offset
                )
                batch["first_seen"] = batch["first_seen"] or now

                # Flush on bucket completion or size thresholds.
                bucket_complete = message.offset >= bucket_end
                size_threshold = len(batch["records"]) >= batch_size or batch["bytes"] >= max_batch_bytes
                if bucket_complete or size_threshold:
                    _flush_batch(key, batch)

        for key, batch in list(batches.items()):
            if batch["records"] and batch["first_seen"] and now - batch["first_seen"] >= max_batch_seconds:
                _flush_batch(key, batch)
            if (
                max_bucket_seconds
                and batch["records"]
                and batch["first_seen"]
                and now - batch["first_seen"] >= max_bucket_seconds
            ):
                _flush_batch(key, batch)
except KeyboardInterrupt:
    print("Interrupted. Flushing pending batches...")
    _flush_all()
finally:
    consumer.close()
