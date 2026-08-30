import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "vendor"))

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from normalizer import normalize

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# env variables
S3_BUCKET = os.environ.get("S3_BUCKET_NAME")

# prefix
BRONZE_PREFIX = "bronze/hacker_news"

def get_yesterday() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)

def build_bronze_prefix(run_date: datetime) -> str:
    return (
        f"{BRONZE_PREFIX}/"
        f"year={run_date.year}/"
        f"month={run_date.month:02d}/"
        f"day={run_date.day:02d}/"
    )

def read_bronze_items(prefix: str, s3_client) -> list[dict]:
    """
    Read all JSON files from bronze S3 folder for a date
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix
        )
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")
        return []

    objects = response.get("Contents", [])
    if not objects:
        logger.warning(f"No bronze files for prefix: {prefix}")
        return []
    
    all_items = []

    for obj in objects:
        key = obj["Key"]

        if not key.endswith(".json"):
            continue

        try:
            # get the json file
            s3_response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)

            # reading body and parsing json
            content = s3_response["Body"].read().decode("utf-8")
            items = json.loads(content)

            if not isinstance(items, list):
                continue

            all_items.extend(items)

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from {key}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error reading {key}: {e}")
            continue
    
    logger.info(f"Read {len(all_items)} bronze items")
    return all_items

def write_posts_to_silver(df: pd.DataFrame, run_date: datetime) -> bool:
    """
    Write posts DataFrrame to silver layer as Parquet
    """
    if df.empty:
        logger.warning("Posts DataFrame is empty, skipping write")
        return True
    
    bucket = os.environ.get("S3_BUCKET_NAME")
    silver_posts_path = f"s3://{bucket}/silver/posts/"
    
    try:
        # adding partition columns for awswrangler to use to create folder structure
        df["year"] = run_date.year
        df["month"] = run_date.month
        df["day"] = run_date.day

        wr.s3.to_parquet(
            df=df,
            path=silver_posts_path,
            # dataset = true, awswrangler treats the path as a folder with multiple files
            dataset=True,
            partition_cols=["year", "month", "day"],
            mode="overwrite_partitions", # overwrite only this partition
        )
        return True
    
    except Exception as e:
        logger.error(f"Error writing posts Parquet: {e}")
        return False
    
def write_users_to_silver(df: pd.DataFrame) -> bool:
    """
    Write users DateFrame to silver layes as Parquet
    """
    if df.empty:
        return True
    
    bucket = os.environ.get("S3_BUCKET_NAME")
    silver_users_path = f"s3://{bucket}/silver/users/"

    try:
        wr.s3.to_parquet(
            df=df,
            path=silver_users_path,
            dataset=True,
            partition_cols=["platform"],
            mode="overwrite_partitions",
        )
        return True
    
    except Exception as e:
        logger.error(f"Error writing users Parquet: {e}")
        return False
    
def compute_data_quality_score(posts_df: pd.DataFrame, users_df: pd.DataFrame) -> dict:
    scores = {}

    for name, df in [("posts", posts_df), ("users", users_df)]:
        if df.empty:
            scores[f"{name}_quality"] = 0.0
            continue
    
        # calculate the percentage of the rows that have no null vals
        non_null_rows = df.notna().all(axis=1).sum()
        total_rows = len(df)
        quality_pct = round((non_null_rows / total_rows) * 100, 2)
        scores[f"{name}_quality"] = quality_pct

    return scores

def handler(event: dict, context) -> dict:
    """
    Lambda entrypoint 

    Can be called two ways either step functions event is empty or has run_dat, or console event is empty
    """

    # get the date for which we are normalizing
    # step function can pass the parameter through event
    # otherwise use yesterday
    if "run_date" in event:
        run_date = datetime.strptime(event["run_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        run_date = get_yesterday()
    
    s3_client = boto3.client("s3")

    try:
        prefix = build_bronze_prefix(run_date)
        items = read_bronze_items(prefix, s3_client)

        if not items:
            logger.warning("No bronze data for normalizing")
            return {
                "statusCode": 200,
                "run_date": str(run_date.date()),
                "posts_count": 0,
                "users_count": 0,
                "message": "No bronze data",
            }
        
        result = normalize(items)
        posts_df, users_df = result
        posts_df, users_df = normalize(items)

        posts_ok = write_posts_to_silver(posts_df, run_date)
        users_ok = write_users_to_silver(users_df)

        quality = compute_data_quality_score(posts_df, users_df)

        return {
            "statusCode": 200,
            "run_date": str(run_date.date()),
            "posts_count": len(posts_df),
            "users_count": len(users_df),
            "posts_written": posts_ok,
            "users_written": users_ok,
            "data_quality": quality,
        }
    
    except Exception as e:
        logger.error(f"Error in HN Silver Lambda: {e}", exc_info=True)
        raise
