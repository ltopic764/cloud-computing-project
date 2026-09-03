import logging
from datetime import date

import awswrangler as wr
import pandas as pd


logger = logging.getLogger(__name__)

PLATFORM_X = "X"


def load_silver_users(
    bucket: str,
) -> pd.DataFrame:
    """
    Read the current X users snapshot from the shared Silver users dataset.
    """

    path = f"s3://{bucket}/silver/users/"

    try:
        df = wr.s3.read_parquet(
            path=path,
            dataset=True,
            partition_filter=lambda partition: (
                partition.get("platform") == PLATFORM_X
            ),
        )

        # Defensive filtering in case platform is still present
        # as a normal DataFrame column after reading.
        if "platform" in df.columns:
            df = df[
                df["platform"] == PLATFORM_X
            ].copy()

        logger.info(
            f"Loaded {len(df)} X users from Silver"
        )

        return df

    except Exception as e:
        logger.error(
            f"Error reading X users from Silver: {e}",
            exc_info=True,
        )
        raise


def load_silver_posts(
    bucket: str,
) -> pd.DataFrame:
    """
    Read X posts from the shared Silver posts dataset.

    Twitter Gold does not need posts for its two main metrics,
    but posts are needed for the X Data Quality Score KPI.
    """

    path = f"s3://{bucket}/silver/posts/"

    try:
        df = wr.s3.read_parquet(
            path=path,
            dataset=True,
        )

        if "platform" not in df.columns:
            logger.warning(
                "Silver posts dataset has no platform column"
            )
            return pd.DataFrame()

        df = df[
            df["platform"] == PLATFORM_X
        ].copy()

        logger.info(
            f"Loaded {len(df)} X posts from Silver"
        )

        return df

    except Exception as e:
        logger.error(
            f"Error reading X posts from Silver: {e}",
            exc_info=True,
        )
        raise


def prepare_users(
    users_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prepare Silver X users for Gold calculations.

    Silver currently already stores followers_count as nullable Int64,
    but numeric conversion is kept here defensively.
    """

    if users_df.empty:
        return users_df.copy()

    df = users_df.copy()

    if "platform" in df.columns:
        df = df[
            df["platform"] == PLATFORM_X
        ].copy()

    if "followers_count" in df.columns:
        df["followers_count"] = pd.to_numeric(
            df["followers_count"],
            errors="coerce",
        ).astype("Int64")

    if "created_at" in df.columns:
        df["created_at_parsed"] = pd.to_datetime(
            df["created_at"],
            utc=True,
            errors="coerce",
        )

    return df


def compute_daily_users_metric(
    users_df: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    """
    Calculate daily X user metric.

    total_users:
        number of unique X usernames currently present in Silver.

    new_users:
        number of X accounts whose created_at date equals run_date.
    """

    columns = [
        "date",
        "platform",
        "total_users",
        "new_users",
    ]

    if users_df.empty:
        return pd.DataFrame(
            [
                {
                    "date": run_date,
                    "platform": PLATFORM_X,
                    "total_users": 0,
                    "new_users": 0,
                }
            ],
            columns=columns,
        )

    df = prepare_users(users_df)

    if "username" not in df.columns:
        raise ValueError(
            "Silver users dataset is missing "
            "required column 'username'"
        )

    total_users = int(
        df["username"]
        .dropna()
        .astype("string")
        .nunique()
    )

    new_users = 0

    if "created_at_parsed" in df.columns:
        new_users = int(
            (
                df["created_at_parsed"].dt.date
                == run_date
            ).sum()
        )

    metric = pd.DataFrame(
        [
            {
                "date": run_date,
                "platform": PLATFORM_X,
                "total_users": total_users,
                "new_users": new_users,
            }
        ]
    )

    logger.info(
        f"daily_users_metric: "
        f"total_users={total_users}, "
        f"new_users={new_users}"
    )

    return metric


def compute_top_users_by_followers(
    users_df: pd.DataFrame,
    run_date: date,
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Calculate top N unique X users by followers_count.
    """

    columns = [
        "date",
        "rank",
        "username",
        "followers_count",
        "is_verified",
    ]

    if users_df.empty:
        return pd.DataFrame(
            columns=columns
        )

    df = prepare_users(users_df)

    required_columns = {
        "username",
        "followers_count",
    }

    missing_columns = (
        required_columns
        - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Silver users dataset is missing "
            f"required columns: "
            f"{sorted(missing_columns)}"
        )

    ranking_df = df[
        df["username"].notna()
        & df["followers_count"].notna()
    ].copy()

    if ranking_df.empty:
        return pd.DataFrame(
            columns=columns
        )

    ranking_df = (
        ranking_df
        .sort_values(
            [
                "followers_count",
                "username",
            ],
            ascending=[
                False,
                True,
            ],
        )
        .drop_duplicates(
            subset=["username"],
            keep="first",
        )
        .head(top_n)
        .reset_index(drop=True)
    )

    ranking_df["rank"] = (
        ranking_df.index + 1
    )

    ranking_df["date"] = run_date

    if "is_verified" not in ranking_df.columns:
        ranking_df["is_verified"] = pd.NA

    result = ranking_df[
        columns
    ]

    logger.info(
        f"top_users_followers: "
        f"{len(result)} users"
    )

    return result


def calculate_quality(
    df: pd.DataFrame,
    required_columns: list[str],
) -> float:
    """
    Calculate percentage of rows which contain all fields
    required for the current platform.

    Shared-schema columns which are intentionally null for X
    are not included.
    """

    if df.empty:
        return 0.0

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        logger.warning(
            "Data Quality required columns "
            f"are missing: {missing_columns}"
        )
        return 0.0

    complete_rows = (
        df[required_columns]
        .notna()
        .all(axis=1)
        .sum()
    )

    quality_pct = round(
        (
            complete_rows
            / len(df)
        )
        * 100,
        2,
    )

    return quality_pct


def compute_data_quality_score(
    posts_df: pd.DataFrame,
    users_df: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    """
    Calculate X Data Quality Score.

    Structural null columns are ignored.

    X posts intentionally have:
        score = null
        num_comments = null
        url = null

    X users intentionally have:
        karma_score = null
    """

    x_posts = posts_df.copy()

    if (
        not x_posts.empty
        and "platform" in x_posts.columns
    ):
        x_posts = x_posts[
            x_posts["platform"] == PLATFORM_X
        ].copy()

    x_users = prepare_users(
        users_df
    )

    posts_quality = calculate_quality(
        x_posts,
        [
            "post_id",
            "author_username",
            "content_text",
            "created_at",
            "post_type",
            "platform",
        ],
    )

    users_quality = calculate_quality(
        x_users,
        [
            "user_id",
            "username",
            "platform",
            "is_verified",
            "followers_count",
            "created_at",
        ],
    )

    return pd.DataFrame(
        [
            {
                "date": run_date,
                "platform": PLATFORM_X,
                "table_name": "posts",
                "quality_pct": posts_quality,
            },
            {
                "date": run_date,
                "platform": PLATFORM_X,
                "table_name": "users",
                "quality_pct": users_quality,
            },
        ]
    )


def run(
    bucket: str,
    run_date: date,
) -> dict[str, pd.DataFrame]:
    """
    Load Silver data and calculate all Twitter Gold metrics.
    """

    users_df = load_silver_users(
        bucket
    )

    posts_df = load_silver_posts(
        bucket
    )

    results = {
        "daily_users_metric":
            compute_daily_users_metric(
                users_df,
                run_date,
            ),

        "top_users_followers":
            compute_top_users_by_followers(
                users_df,
                run_date,
            ),

        "data_quality_score":
            compute_data_quality_score(
                posts_df,
                users_df,
                run_date,
            ),
    }

    for name, df in results.items():
        logger.info(
            f"{name}: {len(df)} rows"
        )

    return results