import logging
import os
import sys
from datetime import datetime, timezone, timedelta, date

import boto3
import awswrangler as wr

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "vendor"))

from transformer import run

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET_NAME")

# prefix
GOLD_PREFIX = "gold/hacker_news"

def get_yesterday() -> date:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()

def write_metric_to_s3(df, metric_name: str, run_date: date, partition_cols: list[str]) -> bool:
    """
    Write one DataFrame as parquet in gold layer
    """
    if df.empty:
        logger.warning(f"{metric_name}: empty DataFrame, skipping write")
        return True
    
    path = f"s3://{S3_BUCKET}/{GOLD_PREFIX}/{metric_name}/"

    try:
        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            partition_cols=partition_cols,
            mode="overwrite_partitions"
        )
        logger.info(f"{metric_name}: written {len(df)} rows to {path}")
        return True
    except Exception as e:
        logger.error(f"Error writing {metric_name}: {e}")
        return True
    
def handler(event: dict, context) -> dict:
    logging.getLogger().setLevel(logging.INFO)

    # get the date
    if "run_date" in event:
        try:
            run_date = datetime.strptime(event["run_date"], "%Y-%m-%d").date()
        except Exception as e:
            run_date = get_yesterday()
            logger.warning(f"Wrong date format, using yesterdays date")
    else:
        run_date = get_yesterday()

    try:
        # calculate all metrics
        metrics = run(S3_BUCKET, run_date)

        # write all of the metrics in S3
        write_config = {
            "daily_posts_metric":   ["date"],
            "daily_users_metric":   ["platform", "date"],
            "top_karma_users_high": ["date"],
            "top_karma_users_low":  ["date"],
            "top_jobs":             ["date"],
            "top_posts":            ["date"],
            "data_quality_score":   ["platform", "date"],
        }

        results = {}
        for metric_name, partition_cols in write_config.items():
            df = metrics.get(metric_name)
            if df is None:
                logger.warning(f"Metric {metric_name} not found in results")
                results[metric_name] = False
                continue

            results[metric_name] = write_metric_to_s3(df, metric_name, run_date, partition_cols)
        
        # summary
        successful = sum(1 for ok in results.values() if ok)
        total = len(results)

        return {
            "statusCode": 200,
            "run_date": str(run_date),
            "metrics_written": results,
            "success_count": successful,
            "total_count": total,
        }
    except Exception as e:
        logger.error(f"Error in HN Gold Lambda: {e}", exc_info=True)
        raise

