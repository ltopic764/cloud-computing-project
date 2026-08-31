import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import awswrangler as wr
import boto3
import pandas as pd

from normalizer import normalize


logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


S3_BUCKET = os.environ["S3_BUCKET_NAME"]

BRONZE_PREFIX = "bronze/twitter"
SILVER_POSTS_PREFIX = "silver/posts"
SILVER_USERS_PREFIX = "silver/users"


# ============================================================
# DATE / BATCH HELPERS
# ============================================================

def get_today() -> datetime:
    return datetime.now(timezone.utc)


def build_bronze_prefix(run_date: datetime) -> str:
    """
    Build Twitter Bronze partition prefix.

    Example:
    bronze/twitter/year=2026/month=08/day=31/
    """
    return (
        f"{BRONZE_PREFIX}/"
        f"year={run_date.year}/"
        f"month={run_date.month:02d}/"
        f"day={run_date.day:02d}/"
    )


def build_batch_id(bronze_keys: list[str]) -> str:
    """
    Create a deterministic ID from Bronze S3 object keys.

    The same Bronze key or the same set of Bronze keys always
    produces the same batch ID.

    This is used for idempotent Silver writes.
    """
    normalized_keys = "|".join(
        sorted(bronze_keys)
    )

    return hashlib.sha256(
        normalized_keys.encode("utf-8")
    ).hexdigest()[:16]


# ============================================================
# EVENT HANDLING
# ============================================================

def extract_bronze_keys_from_event(
    event: dict,
) -> list[str]:
    """
    Extract Twitter Bronze object keys from an AWS event.

    Primary architecture:
        S3 -> EventBridge -> Twitter Silver Lambda

    Direct S3 notification format is also supported as a
    defensive fallback.
    """
    keys = []

    # --------------------------------------------------------
    # EventBridge S3 Object Created event
    # --------------------------------------------------------

    if event.get("source") == "aws.s3":
        detail = event.get(
            "detail",
            {},
        )

        bucket_name = (
            detail
            .get("bucket", {})
            .get("name")
        )

        key = (
            detail
            .get("object", {})
            .get("key")
        )

        if (
            bucket_name == S3_BUCKET
            and key
        ):
            key = unquote_plus(key)

            if (
                key.startswith(
                    f"{BRONZE_PREFIX}/"
                )
                and key.endswith(".jsonl")
            ):
                keys.append(key)

    # --------------------------------------------------------
    # Direct S3 notification fallback
    # --------------------------------------------------------

    for record in event.get(
        "Records",
        [],
    ):
        try:
            bucket_name = (
                record["s3"]["bucket"]["name"]
            )

            key = unquote_plus(
                record["s3"]["object"]["key"]
            )

            if (
                bucket_name == S3_BUCKET
                and key.startswith(
                    f"{BRONZE_PREFIX}/"
                )
                and key.endswith(".jsonl")
            ):
                keys.append(key)

        except KeyError:
            continue

    return list(dict.fromkeys(keys))


# ============================================================
# BRONZE S3 READING
# ============================================================

def list_bronze_keys(
    prefix: str,
    s3_client,
) -> list[str]:
    """
    List all Twitter JSONL Bronze objects under the supplied
    prefix.

    Pagination is used so the code is not limited to the first
    1000 S3 objects.
    """
    keys = []

    paginator = s3_client.get_paginator(
        "list_objects_v2"
    )

    pages = paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
    )

    for page in pages:
        for obj in page.get(
            "Contents",
            [],
        ):
            key = obj["Key"]

            if key.endswith(".jsonl"):
                keys.append(key)

    return keys


def read_jsonl_object(
    key: str,
    s3_client,
) -> list[dict]:
    """
    Read one Twitter Bronze JSONL object.

    Invalid individual JSON lines are logged and skipped instead
    of causing the whole batch to fail.
    """
    response = s3_client.get_object(
        Bucket=S3_BUCKET,
        Key=key,
    )

    content = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    items = []

    for line_number, line in enumerate(
        content.splitlines(),
        start=1,
    ):
        line = line.strip()

        if not line:
            continue

        try:
            item = json.loads(line)

            if isinstance(item, dict):
                items.append(item)

        except json.JSONDecodeError as e:
            logger.warning(
                f"Invalid JSON in {key}, "
                f"line {line_number}: {e}"
            )

    return items


def read_bronze_items(
    keys: list[str],
    s3_client,
) -> list[dict]:
    """
    Read all requested Twitter Bronze JSONL objects.
    """
    all_items = []

    for key in keys:
        try:
            items = read_jsonl_object(
                key,
                s3_client,
            )

            all_items.extend(items)

            logger.info(
                f"Read {len(items)} rows "
                f"from {key}"
            )

        except Exception as e:
            logger.error(
                f"Error reading {key}: {e}",
                exc_info=True,
            )

            raise

    logger.info(
        f"Read {len(all_items)} total "
        f"Twitter Bronze rows"
    )

    return all_items


# ============================================================
# POST PARTITIONING
# ============================================================

def add_post_partition_columns(
    df: pd.DataFrame,
    fallback_date: datetime,
) -> pd.DataFrame:
    """
    Add year/month/day partition columns.

    Normal case:
        partition comes from tweet created_at.

    If created_at is null or invalid:
        the row is NOT deleted.

    Instead, fallback_date is used only to choose the physical
    S3 partition.

    The actual created_at column remains null.
    """
    if df.empty:
        return df

    df = df.copy()

    parsed_dates = pd.to_datetime(
        df["created_at"],
        utc=True,
        errors="coerce",
    )

    fallback_timestamp = pd.Timestamp(
        fallback_date
    )

    parsed_dates = parsed_dates.fillna(
        fallback_timestamp
    )

    df["year"] = pd.array(
        parsed_dates.dt.year,
        dtype="Int64",
    )

    df["month"] = pd.array(
        parsed_dates.dt.month,
        dtype="Int64",
    )

    df["day"] = pd.array(
        parsed_dates.dt.day,
        dtype="Int64",
    )

    return df


# ============================================================
# POSTS IDEMPOTENCY
# ============================================================

def get_existing_batch_files(
    batch_id: str,
) -> list[str]:
    """
    Find Silver Parquet files previously created from exactly
    this Twitter Bronze batch.

    HackerNews files and other Twitter batches are not matched.
    """
    silver_posts_path = (
        f"s3://{S3_BUCKET}/"
        f"{SILVER_POSTS_PREFIX}/"
    )

    existing_objects = wr.s3.list_objects(
        path=silver_posts_path,
        suffix=".parquet",
    )

    expected_prefix = (
        f"twitter_{batch_id}_"
    )

    return [
        path
        for path in existing_objects
        if (
            path
            .split("/")[-1]
            .startswith(expected_prefix)
        )
    ]


def delete_existing_batch_files(
    batch_id: str,
) -> None:
    """
    Remove a previous output for this exact Bronze batch.

    This makes an EventBridge retry idempotent.

    Example:

        first run:
            twitter_abc123_....parquet

        retry:
            old twitter_abc123 files are removed
            and the batch is written again.

    HackerNews output is never deleted.
    """
    files_to_delete = (
        get_existing_batch_files(
            batch_id
        )
    )

    if not files_to_delete:
        return

    logger.info(
        f"Deleting "
        f"{len(files_to_delete)} "
        f"previous Silver files "
        f"for Twitter batch {batch_id}"
    )

    wr.s3.delete_objects(
        path=files_to_delete
    )


# ============================================================
# GLOBAL POST DEDUPLICATION
# ============================================================

def get_existing_post_ids() -> set[str]:
    """
    Read existing post IDs from the complete shared Silver
    posts dataset.

    Only the post_id column is loaded.

    This allows us to remove duplicates not only inside one
    Bronze dataset, but also between different Bronze datasets.

    For the small student-project dataset this approach is
    simple and inexpensive.
    """
    silver_posts_path = (
        f"s3://{S3_BUCKET}/"
        f"{SILVER_POSTS_PREFIX}/"
    )

    parquet_files = wr.s3.list_objects(
        path=silver_posts_path,
        suffix=".parquet",
    )

    if not parquet_files:
        return set()

    try:
        existing_df = wr.s3.read_parquet(
            path=parquet_files,
            columns=[
                "post_id"
            ],
        )

    except Exception as e:
        logger.warning(
            "Could not read existing post IDs "
            f"for global deduplication: {e}"
        )

        raise

    if (
        existing_df.empty
        or "post_id" not in existing_df.columns
    ):
        return set()

    return set(
        existing_df[
            "post_id"
        ]
        .dropna()
        .astype(str)
        .tolist()
    )


def remove_existing_posts(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Remove posts that already exist in Silver.

    This handles overlap between different Bronze datasets.

    Example:

        dataset A = rows 1-5000
        dataset B = rows 4500-9500

    Posts 4500-5000 already have the same deterministic post_id,
    therefore they are skipped when dataset B is processed.
    """
    if df.empty:
        return df

    existing_ids = (
        get_existing_post_ids()
    )

    if not existing_ids:
        return df

    before = len(df)

    result = df[
        ~df["post_id"]
        .astype("string")
        .isin(existing_ids)
    ].copy()

    removed = (
        before - len(result)
    )

    if removed:
        logger.info(
            f"Removed {removed} posts that "
            f"already existed in Silver"
        )

    return result


# ============================================================
# POSTS SILVER WRITE
# ============================================================

def write_posts_to_silver(
    df: pd.DataFrame,
    fallback_date: datetime,
    batch_id: str,
) -> bool:
    """
    Write Twitter posts into the shared Silver posts dataset.

    Safety mechanisms:

    1. Previous files from the same Bronze batch are deleted.
       -> protects against retries.

    2. Existing post_id values are read from Silver.
       -> protects against overlap between different datasets.

    3. mode='append' is used.
       -> Twitter never overwrites HackerNews daily partitions.
    """
    if df.empty:
        logger.warning(
            "Twitter posts DataFrame is empty, "
            "skipping posts write"
        )
        return True

    silver_posts_path = (
        f"s3://{S3_BUCKET}/"
        f"{SILVER_POSTS_PREFIX}/"
    )

    try:
        # ----------------------------------------------------
        # STEP 1:
        # Remove old output from this exact Bronze batch.
        # ----------------------------------------------------

        delete_existing_batch_files(
            batch_id
        )

        # ----------------------------------------------------
        # STEP 2:
        # Remove posts that already exist from other batches.
        # ----------------------------------------------------

        deduplicated_df = (
            remove_existing_posts(
                df
            )
        )

        if deduplicated_df.empty:
            logger.info(
                "All posts from this Twitter "
                "batch already exist in Silver"
            )

            return True

        # ----------------------------------------------------
        # STEP 3:
        # Add S3 partition columns.
        # ----------------------------------------------------

        output_df = (
            add_post_partition_columns(
                deduplicated_df,
                fallback_date,
            )
        )

        # ----------------------------------------------------
        # STEP 4:
        # Append only new Twitter posts.
        # ----------------------------------------------------

        wr.s3.to_parquet(
            df=output_df,
            path=silver_posts_path,
            dataset=True,

            partition_cols=[
                "year",
                "month",
                "day",
            ],

            mode="append",

            filename_prefix=(
                f"twitter_{batch_id}_"
            ),
        )

        logger.info(
            f"Wrote "
            f"{len(deduplicated_df)} "
            f"new Twitter posts "
            f"for batch {batch_id}"
        )

        return True

    except Exception as e:
        logger.error(
            f"Error writing Twitter "
            f"posts Parquet: {e}",
            exc_info=True,
        )

        return False


# ============================================================
# USERS
# ============================================================

def read_existing_x_users() -> pd.DataFrame:
    """
    Read existing X users from Silver.

    HackerNews users are stored in another platform partition
    and are never read or modified here.
    """
    x_users_path = (
        f"s3://{S3_BUCKET}/"
        f"{SILVER_USERS_PREFIX}/"
        f"platform=X/"
    )

    existing_files = wr.s3.list_objects(
        path=x_users_path,
        suffix=".parquet",
    )

    if not existing_files:
        return pd.DataFrame()

    return wr.s3.read_parquet(
        path=x_users_path,
        dataset=True,
    )


def merge_x_users(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge previous and new X users.

    Existing users are preserved when a new Bronze file arrives.

    Duplicate usernames are reduced to one row.

    Completely null / anonymous duplicate rows are also
    deduplicated by their complete row content.
    """
    if existing_df.empty:
        combined = new_df.copy()

    elif new_df.empty:
        combined = existing_df.copy()

    else:
        combined = pd.concat(
            [
                existing_df,
                new_df,
            ],
            ignore_index=True,
        )

    if combined.empty:
        return combined

    # Remove exact duplicate rows first.
    combined = combined.drop_duplicates(
        keep="last"
    )

    # --------------------------------------------------------
    # For users with username:
    # keep one row per username.
    #
    # keep="last" means that newer Bronze data can update
    # fields such as followers_count or is_verified.
    # --------------------------------------------------------

    if "username" in combined.columns:

        users_with_username = combined[
            combined["username"].notna()
        ].drop_duplicates(
            subset=[
                "username",
                "platform",
            ],
            keep="last",
        )

        users_without_username = combined[
            combined["username"].isna()
        ]

        combined = pd.concat(
            [
                users_with_username,
                users_without_username,
            ],
            ignore_index=True,
        )

    return combined


def enforce_user_dtypes(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Restore the common nullable Silver schema after concatenating
    old and new user DataFrames.
    """
    if df.empty:
        return df

    df = df.copy()

    for column in [
        "user_id",
        "username",
        "platform",
        "created_at",
    ]:
        if column in df.columns:
            df[column] = pd.array(
                df[column],
                dtype="string",
            )

    if "karma_score" in df.columns:
        df["karma_score"] = pd.array(
            df["karma_score"],
            dtype="Int64",
        )

    if "is_verified" in df.columns:
        df["is_verified"] = pd.array(
            df["is_verified"],
            dtype="boolean",
        )

    if "followers_count" in df.columns:
        df["followers_count"] = pd.array(
            df["followers_count"],
            dtype="Int64",
        )

    return df


def write_users_to_silver(
    new_df: pd.DataFrame,
) -> bool:
    """
    Merge new Twitter users with existing Twitter users and
    overwrite only the platform=X partition.

    HackerNews partition is not modified.
    """
    if new_df.empty:
        logger.warning(
            "Twitter users DataFrame is empty, "
            "skipping users write"
        )
        return True

    silver_users_path = (
        f"s3://{S3_BUCKET}/"
        f"{SILVER_USERS_PREFIX}/"
    )

    try:
        existing_df = (
            read_existing_x_users()
        )

        merged_df = merge_x_users(
            existing_df,
            new_df,
        )

        merged_df = enforce_user_dtypes(
            merged_df
        )

        if merged_df.empty:
            return True

        wr.s3.to_parquet(
            df=merged_df,
            path=silver_users_path,
            dataset=True,

            partition_cols=[
                "platform"
            ],

            mode="overwrite_partitions",

            filename_prefix="twitter_",
        )

        logger.info(
            f"Wrote "
            f"{len(merged_df)} "
            f"total X users to Silver"
        )

        return True

    except Exception as e:
        logger.error(
            f"Error writing Twitter "
            f"users Parquet: {e}",
            exc_info=True,
        )

        return False


# ============================================================
# DATA QUALITY
# ============================================================

def compute_data_quality_score(
    posts_df: pd.DataFrame,
    users_df: pd.DataFrame,
) -> dict:
    """
    Same basic quality calculation currently used by the
    HackerNews Silver implementation.

    A completely non-null row counts as a valid row.

    Because the shared schema intentionally contains
    platform-specific null fields, X quality can be lower.
    """
    scores = {}

    for name, df in [
        ("posts", posts_df),
        ("users", users_df),
    ]:

        if df.empty:
            scores[
                f"{name}_quality"
            ] = 0.0

            continue

        non_null_rows = (
            df.notna()
            .all(axis=1)
            .sum()
        )

        total_rows = len(df)

        quality_pct = round(
            (
                non_null_rows
                / total_rows
            ) * 100,
            2,
        )

        scores[
            f"{name}_quality"
        ] = quality_pct

    return scores


# ============================================================
# LAMBDA HANDLER
# ============================================================

def handler(
    event: dict,
    context,
) -> dict:
    """
    Twitter/X Silver Lambda.

    Normal execution:

        Twitter Bronze writes JSONL
                  |
                  v
             EventBridge
                  |
                  v
        Twitter Silver Lambda
                  |
                  v
        normalized Parquet Silver data

    Manual execution is also supported:

        {
            "run_date": "2026-08-31"
        }
    """

    logger.info(
        "Twitter Silver Lambda started"
    )

    logger.info(
        json.dumps(event)
    )

    s3_client = boto3.client(
        "s3"
    )

    try:

        # ====================================================
        # 1. Determine Bronze files
        # ====================================================

        bronze_keys = (
            extract_bronze_keys_from_event(
                event
            )
        )

        # ----------------------------------------------------
        # EventBridge invocation
        # ----------------------------------------------------

        if bronze_keys:

            run_date = get_today()

        # ----------------------------------------------------
        # Manual invocation / fallback
        # ----------------------------------------------------

        else:

            if "run_date" in event:

                run_date = datetime.strptime(
                    event["run_date"],
                    "%Y-%m-%d",
                ).replace(
                    tzinfo=timezone.utc
                )

            else:
                run_date = get_today()

            prefix = build_bronze_prefix(
                run_date
            )

            bronze_keys = list_bronze_keys(
                prefix,
                s3_client,
            )

        # ====================================================
        # 2. Nothing to process
        # ====================================================

        if not bronze_keys:

            logger.warning(
                "No Twitter Bronze files found"
            )

            return {
                "statusCode": 200,
                "posts_count": 0,
                "users_count": 0,
                "message":
                    "No Twitter Bronze data",
            }

        # ====================================================
        # 3. Stable batch ID
        # ====================================================

        batch_id = build_batch_id(
            bronze_keys
        )

        logger.info(
            f"Twitter Silver batch ID: "
            f"{batch_id}"
        )

        # ====================================================
        # 4. Read Bronze
        # ====================================================

        items = read_bronze_items(
            bronze_keys,
            s3_client,
        )

        if not items:

            logger.warning(
                "Twitter Bronze files "
                "contained no rows"
            )

            return {
                "statusCode": 200,
                "batch_id": batch_id,
                "posts_count": 0,
                "users_count": 0,
                "message":
                    "No Twitter Bronze rows",
            }

        # ====================================================
        # 5. Normalize
        # ====================================================

        posts_df, users_df = normalize(
            items
        )

        logger.info(
            f"Normalized "
            f"{len(posts_df)} posts "
            f"and "
            f"{len(users_df)} users"
        )

        # ====================================================
        # 6. Data Quality
        # ====================================================

        quality = (
            compute_data_quality_score(
                posts_df,
                users_df,
            )
        )

        # ====================================================
        # 7. Write Silver posts
        # ====================================================

        posts_ok = write_posts_to_silver(
            posts_df,
            run_date,
            batch_id,
        )

        # ====================================================
        # 8. Write / merge X users
        # ====================================================

        users_ok = write_users_to_silver(
            users_df
        )

        # ====================================================
        # 9. Validate output
        # ====================================================

        if (
            not posts_ok
            or not users_ok
        ):
            raise RuntimeError(
                "Failed writing Twitter "
                "Silver Parquet files"
            )

        # ====================================================
        # 10. Successful response
        # ====================================================

        return {
            "statusCode": 200,

            "batch_id":
                batch_id,

            "bronze_files":
                len(bronze_keys),

            "bronze_rows":
                len(items),

            "posts_count":
                len(posts_df),

            "users_count":
                len(users_df),

            "posts_written":
                posts_ok,

            "users_written":
                users_ok,

            "data_quality":
                quality,
        }

    except Exception as e:

        logger.error(
            f"Error in Twitter "
            f"Silver Lambda: {e}",
            exc_info=True,
        )

        raise