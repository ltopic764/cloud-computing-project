import sys
import os

# Adding vendor/ to python path
# vendor/ holds dependencies for lambda
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "vendor"))

import logging
import json
from datetime import datetime, timezone, timedelta

import boto3
import requests

from hn_client import fetch_all

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# S3 bucket name
S3_BUCKET = os.environ.get("S3_BUCKET_NAME")

def build_s3_key(item_type: str, run_date: str) -> str:
    """
    Builds S3 key (path) for specific item type and date
    """
    date = datetime.strptime(run_date, "%Y-%m-%d")

    return (
        f"bronze/hacker_news/"
        f"year={date.year}/"
        f"month={date.month:02d}/" 
        f"day={date.day:02d}/"
        f"{item_type}.json"
    )

def upload_to_s3(s3_client, data: list[dict], item_type: str, run_date: str) -> bool:
    """
    Saves item list as JSON file in S3 bucket in the bronze layer
    """
    # If no data, do not write an empty file, save storage
    if not data:
        logger.info(f"No data for {item_type}, skipping entry")
        return True
    
    key = build_s3_key(item_type, run_date)

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,

            Key=key,

            Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),

            ContentType="application/json",
        )

        logger.info(f"Written {len(data)} items - s3://{S3_BUCKET}/{key}")
        return True
    
    except Exception as e:
        logger.error(f"Error while writing {item_type} in S3: {e}")
        return False
    
def handler(event: dict, context) -> dict:
    """
    Lambda entrypoint
    """
    logger.info("HN Bronze Lambda started")

    # aDate for which we fetch data
    from datetime import datetime, timezone, timedelta

    run_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
    logger.info(f"Run date: {run_date}")

    s3_client = boto3.client("s3")
    session = requests.Session()

    try:
        # Get all item types
        all_items = fetch_all(session)

        # Track how many item types successfully saved
        success_count = 0
        failed_types = []

        # Save every type in separate S3 file
        for item_type, items in all_items.items():
            success = upload_to_s3(s3_client, items, item_type, run_date)

            if success:
                success_count += 1
            else:
                failed_types.append(item_type)

        # Log summary
        logger.info(f"Finished: {success_count}/5 types successfully saved")

        if failed_types:
            logger.warning(f"Unsuccessful types: {failed_types}")

        return {
            "statusCode": 200,
            "run_date": run_date,
            "items_fetched": {
                item_type: len(items)
                for item_type, items in all_items.items()
            },
            "failed_types": failed_types
        }
    
    except Exception as e:
        logger.error(f"Error in HN lambda: {e}", exc_info=True)
        raise
