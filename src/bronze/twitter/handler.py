import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

# Safety stop is a buffer time to ensure Lambda has enough time
# to gracefully stop before AWS forcibly terminates it.
SAFETY_STOP_MS = int(os.environ.get("SAFETY_STOP_MS", "15000"))

# Max number of rows to process in one run.
MAX_ROWS_PER_RUN = int(os.environ.get("TWITTER_MAX_ROWS_PER_RUN", "5000"))


def build_bronze_key(source_key: str) -> str:
    now = datetime.now(timezone.utc)

    safe_name = source_key.split("/")[-1].replace(".csv", "")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    return (
        "bronze/twitter/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{safe_name}_{timestamp}.jsonl"
    )


def should_stop(context) -> bool:
    if context is None:
        return False

    return context.get_remaining_time_in_millis() <= SAFETY_STOP_MS


def extract_s3_objects(event) -> list[dict]:
    """
    Supports both:
    - direct S3 notification format
    - EventBridge S3 Object Created format
    """

    objects = []

    # Direct S3 notification format
    if "Records" in event:
        for record in event.get("Records", []):
            bucket = record["s3"]["bucket"]["name"]
            key = unquote_plus(record["s3"]["object"]["key"])

            objects.append(
                {
                    "bucket": bucket,
                    "key": key,
                }
            )

    # EventBridge S3 Object Created format
    elif event.get("source") == "aws.s3":
        bucket = event["detail"]["bucket"]["name"]
        key = unquote_plus(event["detail"]["object"]["key"])

        objects.append(
            {
                "bucket": bucket,
                "key": key,
            }
        )

    else:
        logger.warning("Unsupported event format. No S3 objects extracted.")

    return objects


def read_csv_from_s3(bucket: str, key: str, context) -> tuple[list[str], int]:
    """
    Reads CSV from S3 and converts each row to raw JSON line.

    Important:
    - no normalization
    - no cleaning
    - no schema transformation
    - values stay as strings, exactly as CSV gives them
    """

    response = s3.get_object(Bucket=bucket, Key=key)

    body = response["Body"]
    text_stream = io.TextIOWrapper(body, encoding="utf-8", newline="")

    reader = csv.DictReader(text_stream)

    lines = []
    row_count = 0

    for row in reader:
        if should_stop(context):
            logger.warning("Stopping before Lambda timeout.")
            break

        if row_count >= MAX_ROWS_PER_RUN:
            logger.warning("Stopping because TWITTER_MAX_ROWS_PER_RUN limit was reached.")
            break

        # Raw record wrapper.
        # Original CSV row is preserved under "raw".
        record = {
            "source": "X",
            "source_format": "csv",
            "source_s3_key": key,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "raw": row,
        }

        lines.append(json.dumps(record, ensure_ascii=False))
        row_count += 1

    return lines, row_count


def write_jsonl_to_bronze(bucket: str, key: str, lines: list[str]) -> None:
    body = "\n".join(lines)

    if body:
        body += "\n"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def handler(event, context):
    logger.info("Twitter Bronze Lambda started")
    logger.info(json.dumps(event))

    processed_files = []

    s3_objects = extract_s3_objects(event)

    for s3_object in s3_objects:
        source_bucket = s3_object["bucket"]
        source_key = s3_object["key"]

        if not source_key.startswith("input/twitter/"):
            logger.info(f"Skipping non-twitter input file: {source_key}")
            continue

        if not source_key.lower().endswith(".csv"):
            logger.info(f"Skipping non-csv file: {source_key}")
            continue

        logger.info(f"Reading Twitter dataset from s3://{source_bucket}/{source_key}")

        lines, row_count = read_csv_from_s3(source_bucket, source_key, context)

        if row_count == 0:
            logger.warning(f"No rows loaded from {source_key}")
            continue

        bronze_key = build_bronze_key(source_key)

        write_jsonl_to_bronze(BUCKET_NAME, bronze_key, lines)

        logger.info(
            f"Loaded {row_count} rows from s3://{source_bucket}/{source_key} "
            f"to s3://{BUCKET_NAME}/{bronze_key}"
        )

        processed_files.append(
            {
                "source_key": source_key,
                "bronze_key": bronze_key,
                "rows_loaded": row_count,
            }
        )

    return {
        "statusCode": 200,
        "processed_files": processed_files,
    }