import logging
import os

import boto3
import awswrangler as wr
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


S3_BUCKET = os.environ["S3_BUCKET_NAME"]

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "social_media")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "pipeline_user")

PASSWORD_PARAMETER = os.environ.get(
    "POSTGRES_PASSWORD_PARAMETER",
    "/pipeline/postgres-password",
)


# ============================================================
# DEFINICIJA:
# PostgreSQL tabela -> Gold S3 dataset-i koji je pune
# ============================================================

TABLE_CONFIG = {
    "hn_daily_posts_metric": [
        {
            "path": "gold/hacker_news/daily_posts_metric/",
            "columns": [
                "date",
                "post_type",
                "count",
                "platform",
            ],
        },
    ],

    # Zajednička tabela za HN + Twitter.
    "daily_users_metric": [
        {
            "path": "gold/hacker_news/daily_users_metric/",
            "columns": [
                "date",
                "platform",
                "total_users",
                "new_users",
            ],
        },

        # Očekivana putanja Člana 2 prema uputstvu.
        {
            "path": "gold/twitter/daily_users_metric/",
            "columns": [
                "date",
                "platform",
                "total_users",
                "new_users",
            ],
        },
    ],

    "hn_top_karma_users": [
        {
            "path": "gold/hacker_news/top_karma_users_high/",
            "columns": [
                "date",
                "rank",
                "username",
                "karma_score",
            ],
        },
    ],

    "hn_bottom_karma_users": [
        {
            "path": "gold/hacker_news/top_karma_users_low/",
            "columns": [
                "date",
                "rank",
                "username",
                "karma_score",
            ],
        },
    ],

    "hn_top_jobs": [
        {
            "path": "gold/hacker_news/top_jobs/",
            "columns": [
                "date",
                "rank",
                "post_id",
                "content_text",
                "score",
                "url",
            ],
        },
    ],

    "hn_top_posts": [
        {
            "path": "gold/hacker_news/top_posts/",
            "columns": [
                "date",
                "rank",
                "post_id",
                "content_text",
                "score",
                "url",
                "post_type",
            ],
        },
    ],

    "twitter_top_followers": [
        {
            "path": "gold/twitter/top_users_followers/",
            "columns": [
                "date",
                "rank",
                "username",
                "followers_count",
                "is_verified",
            ],
        },
    ],

    "data_quality_score": [
        {
            "path": "gold/hacker_news/data_quality_score/",
            "columns": [
                "date",
                "platform",
                "table_name",
                "quality_pct",
            ],
        },
        {
            "path": "gold/twitter/data_quality_score/",
            "columns": [
                "date",
                "platform",
                "table_name",
                "quality_pct",
            ],
        },
    ],
}


def get_postgres_password() -> str:
    """
    Reads PostgreSQL password from AWS SSM Parameter Store.
    """

    ssm = boto3.client("ssm")

    response = ssm.get_parameter(
        Name=PASSWORD_PARAMETER,
        WithDecryption=True,
    )

    return response["Parameter"]["Value"]


def get_connection():
    """
    Creates PostgreSQL connection.
    """

    password = get_postgres_password()

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=password,
        connect_timeout=10,
    )


def read_gold_dataset(relative_path: str) -> pd.DataFrame:
    """
    Reads one complete Parquet dataset from Gold layer.

    If the dataset does not exist yet, returns an empty DataFrame.
    """

    s3_path = f"s3://{S3_BUCKET}/{relative_path}"

    try:
        logger.info(f"Reading {s3_path}")

        df = wr.s3.read_parquet(
            path=s3_path,
            dataset=True,
        )

        logger.info(
            f"Read {len(df)} rows from {s3_path}"
        )

        return df

    except Exception as exc:
        logger.warning(
            f"Could not read {s3_path}. "
            f"It may not exist yet. Error: {exc}"
        )

        return pd.DataFrame()


def normalize_dataframe(
    df: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """
    Keeps only expected columns and converts NaN to None
    so psycopg2 can insert SQL NULL.
    """

    if df.empty:
        return df

    missing_columns = [
        column
        for column in columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns: {missing_columns}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df[columns].copy()

    # Convert pandas NaN / NaT to Python None.
    df = df.astype(object).where(
        pd.notnull(df),
        None,
    )

    return df


def insert_dataframe(
    conn,
    table_name: str,
    df: pd.DataFrame,
    columns: list[str],
) -> int:
    """
    Inserts a complete DataFrame into PostgreSQL.
    """

    if df.empty:
        return 0

    column_sql = ", ".join(columns)

    query = (
        f"INSERT INTO {table_name} "
        f"({column_sql}) VALUES %s"
    )

    rows = [
        tuple(row)
        for row in df.itertuples(
            index=False,
            name=None,
        )
    ]

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            query,
            rows,
            page_size=1000,
        )

    return len(rows)


def load_table(
    conn,
    table_name: str,
    sources: list[dict],
) -> int:
    """
    Loads all configured S3 datasets into one PostgreSQL table.

    We reload the whole table each time so repeated Step Functions
    executions do not create duplicate records.
    """

    frames = []
    expected_columns = None

    for source in sources:
        columns = source["columns"]

        if expected_columns is None:
            expected_columns = columns

        df = read_gold_dataset(
            source["path"]
        )

        if df.empty:
            continue

        df = normalize_dataframe(
            df,
            columns,
        )

        frames.append(df)

    if not frames:
        logger.warning(
            f"No data found for table {table_name}. "
            "Existing DB rows will NOT be deleted."
        )

        return 0

    final_df = pd.concat(
        frames,
        ignore_index=True,
    )

    with conn.cursor() as cursor:
        cursor.execute(
            f"TRUNCATE TABLE {table_name};"
        )

    inserted = insert_dataframe(
        conn,
        table_name,
        final_df,
        expected_columns,
    )

    logger.info(
        f"{table_name}: inserted {inserted} rows"
    )

    return inserted


def handler(event, context):
    """
    AWS Lambda entry point.

    Reads Gold Parquet datasets from S3 and loads them
    into PostgreSQL running on EC2.
    """

    logger.info(
        f"Starting DB Loader. Event: {event}"
    )

    connection = None

    try:
        connection = get_connection()

        results = {}

        for table_name, sources in TABLE_CONFIG.items():
            inserted = load_table(
                connection,
                table_name,
                sources,
            )

            results[table_name] = inserted

        connection.commit()

        total_rows = sum(
            results.values()
        )

        logger.info(
            f"DB Loader finished. "
            f"Total rows inserted: {total_rows}"
        )

        return {
            "statusCode": 200,
            "message": "Gold layer loaded into PostgreSQL",
            "tables": results,
            "total_rows": total_rows,
        }

    except Exception as exc:
        if connection is not None:
            connection.rollback()

        logger.error(
            f"DB Loader failed: {exc}",
            exc_info=True,
        )

        raise

    finally:
        if connection is not None:
            connection.close()