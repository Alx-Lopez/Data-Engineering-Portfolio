import io
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

minio_endpoint_url = os.getenv("MINIO_ENDPOINT_URL")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")

prefix = os.getenv("COMPACT_PREFIX", "").strip().strip("/")  # topic=.../dt=.../hr=.../partition=...
compact_date = os.getenv("COMPACT_DATE", "").strip()  # YYYY-MM-DD
compact_hour = os.getenv("COMPACT_HOUR", "").strip()  # HH
max_files = int(os.getenv("COMPACT_MAX_FILES", "200"))
compression = os.getenv("PARQUET_COMPRESSION", "snappy")
delete_sources = os.getenv("COMPACT_DELETE_SOURCES", "false").lower() == "true"

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

def _list_parquet_keys(prefix_value):
    keys = []
    token = None
    while True:
        params = {"Bucket": minio_bucket_name, "Prefix": f"{prefix_value}/"}
        if token:
            params["ContinuationToken"] = token
        response = s3.list_objects_v2(**params)
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
    return sorted(keys)


def _compact_prefix(prefix_value):
    keys = _list_parquet_keys(prefix_value)[:max_files]
    if not keys:
        print(f"No parquet files found for prefix: {prefix_value}")
        return

    buffers = []
    for key in keys:
        body = s3.get_object(Bucket=minio_bucket_name, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        buffers.append(table)

    # Merge into a single compacted file for faster downstream reads.
    combined = pa.concat_tables(buffers)
    output_key = f"{prefix_value}/compacted_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.parquet"

    out_buffer = io.BytesIO()
    pq.write_table(combined, out_buffer, compression=compression)
    out_buffer.seek(0)

    s3.put_object(
        Bucket=minio_bucket_name,
        Key=output_key,
        Body=out_buffer.read(),
        ContentType="application/x-parquet",
    )

    if delete_sources:
        s3.delete_objects(
            Bucket=minio_bucket_name,
            Delete={"Objects": [{"Key": key} for key in keys]},
        )

    print(f"Compacted {len(keys)} files into {output_key}")


def _latest_dt_hr_from_keys(keys):
    latest = None
    for key in keys:
        parts = key.split("/")
        dt = next((p.replace("dt=", "") for p in parts if p.startswith("dt=")), None)
        hr = next((p.replace("hr=", "") for p in parts if p.startswith("hr=")), None)
        if dt and hr:
            candidate = (dt, hr)
            if latest is None or candidate > latest:
                latest = candidate
    return latest


def _partition_prefixes_for_dt_hr(dt_value, hr_value):
    search_prefix = f"dt={dt_value}/hr={hr_value}/"
    keys = _list_parquet_keys("")
    prefixes = set()
    for key in keys:
        if search_prefix not in key:
            continue
        parts = key.split("/")
        if "dt=" not in key or "hr=" not in key:
            continue
        if "partition=" not in key:
            continue
        prefix_parts = []
        for part in parts:
            prefix_parts.append(part)
            if part.startswith("partition="):
                break
        prefixes.add("/".join(prefix_parts))
    return sorted(prefixes)


if prefix:
    _compact_prefix(prefix)
else:
    keys = _list_parquet_keys("")
    if not keys:
        print("No parquet files found in bucket.")
        raise SystemExit(0)

    if compact_date and compact_hour:
        dt_value, hr_value = compact_date, compact_hour
    elif compact_date and not compact_hour:
        dt_value, hr_value = compact_date, None
    else:
        latest = _latest_dt_hr_from_keys(keys)
        if not latest:
            print("No dt/hr partitions found.")
            raise SystemExit(0)
        dt_value, hr_value = latest

    if hr_value:
        prefixes = _partition_prefixes_for_dt_hr(dt_value, hr_value)
    else:
        prefixes = []
        for hr in [f"{h:02d}" for h in range(24)]:
            prefixes.extend(_partition_prefixes_for_dt_hr(dt_value, hr))

    if not prefixes:
        print("No partition prefixes found to compact.")
        raise SystemExit(0)

    for p in prefixes:
        _compact_prefix(p)
