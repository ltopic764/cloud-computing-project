import logging
import pandas as pd
import awswrangler as wr
from datetime import date

logger = logging.getLogger(__name__)

PLATFORM_HN = "HackerNews"

def load_silver_posts(bucket: str, run_date: date) -> pd.DataFrame:
    """
    Read HN posts from the silver layer for a given date
    """
    path = f"s3://{bucket}/silver/posts/"

    try:
        df = wr.s3.read_parquet(
            path=path,
            dataset=True,
            # filter only HN and by date
            partition_filter=lambda x: (
                x.get("year") == str(run_date.year) and
                x.get("month") == str(run_date.month) and
                x.get("day") == str(run_date.day)
            )
        )

        if "platform" in df.columns:
            df = df[df["platform"] == PLATFORM_HN]
        
        logger.info(f"Loaded {len(df)} HN posts for {run_date}")
        return df
    except Exception as e:
        logger.error(f"Error reading silver posts: {e}")
        return pd.DataFrame(columns=[
            "post_id", "author_username", "content_text",
            "created_at", "post_type", "score", "num_comments",
            "platform", "url"
        ])

def load_silver_users(bucket: str) -> pd.DataFrame:
    """
    Read HN users from the silver layer for a given date
    """
    path = f"s3://{bucket}/silver/users/"

    try:
        df = wr.s3.read_parquet(
            path=path,
            dataset=True,
            partition_filter=lambda x : x.get("platform") == PLATFORM_HN
        )

        logger.info(f"Loaded {len(df)} HN users")
        return df
    except Exception as e:
        logger.error(f"Error reading silver users: {e}")
        return pd.DataFrame(columns=[
            "user_id", "username", "platform",
            "karma_score", "is_verified", "followers_count", "created_at"
        ])

def compute_daily_posts_metric(posts_df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """
    How many posts are created daily for each post type
    """
    if posts_df.empty:
        logger.warning("Posts DataFrame is empty for daily_posts_metric")
        return pd.DataFrame(columns=["date", "post_type", "count", "platform"])
    
    # group by post type 
    metric = (
        posts_df
        .groupby("post_type")
        .size() # num of rows per col
        .reset_index(name="count")
    )

    metric["date"] = run_date
    metric["platform"] = PLATFORM_HN

    # cols
    metric = metric[["date", "post_type", "count", "platform"]]

    logger.info(f"daily_posts_metric: {len(metric)} post types, total {metric['count'].sum()} posts")
    return metric

def compute_daily_users_metric(users_df: pd.DataFrame, run_date:date) -> pd.DataFrame:
    """
    Total HN Users on a daily basis
    """
    if users_df.empty:
        logger.warning("Users DataFrame is empty for daily_users_metric")
        return pd.DataFrame(columns=["date", "platform", "total_users", "new_users"])
    
    total_users = len(users_df)

    run_date_str = str(run_date)
    new_users = users_df[
        users_df["created_at"].fillna("").str.startswith(run_date_str)
    ].shape[0]

    metric = pd.DataFrame([{
        "date": run_date,
        "platform": PLATFORM_HN,
        "total_users": total_users,
        "new_users": new_users,
    }])

    logger.info(f"daily_users_metric: total={total_users}, new={new_users}")
    return metric

def compute_top_karma_users(users_df: pd.DataFrame, run_date: date, top_n: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty_df = pd.DataFrame(columns=["date", "rank", "username", "karma_score"])

    if users_df.empty:
        return empty_df, empty_df
    
    # filter users with karma score
    karma_df = users_df[users_df["karma_score"].notna()].copy()

    if karma_df.empty:
        logger.warning("No users with karma scores")
        return empty_df, empty_df
    
    karma_df["karma_score"] = pd.to_numeric(karma_df["karma_score"], errors="coerce")

    def build_ranking(df: pd.DataFrame, ascending: bool) -> pd.DataFrame:
        """Helper for sorting and adding rank col"""
        ranked = (
            df
            .sort_values("karma_score", ascending=ascending)
            .head(top_n)
            .reset_index(drop=True)
        )
        ranked["rank"] = ranked.index + 1
        ranked["date"] = run_date
        return ranked[["date", "rank", "username", "karma_score"]]
    
    top_high = build_ranking(karma_df, ascending=False) # highest karma
    top_low = build_ranking(karma_df, ascending=True) # lowest karma

    logger.info(f"top_karma: high={len(top_high)}, low={len(top_low)} users")
    return top_high, top_low

def compute_top_jobs(posts_df: pd.DataFrame, run_date: date, top_n: int = 10) -> pd.DataFrame:
    """
    Top 10 job offers with the highest score
    """
    empty_df = pd.DataFrame(columns=["date", "rank", "post_id", "content_text", "score", "url"])

    if posts_df.empty:
        return empty_df
    
    # filtering
    jobs_df = posts_df[
        (posts_df["post_type"] == "job") &
        (posts_df["score"].notna())
    ].copy()

    if jobs_df.empty:
        logger.warning("No job posts for top_jobs metric")
        return empty_df

    jobs_df["score"] = pd.to_numeric(jobs_df["score"], errors="coerce")

    top = (
        jobs_df
        .sort_values("score", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    top["rank"] = top.index + 1
    top["date"] = run_date

    result = top[["date", "rank", "post_id", "content_text", "score", "url"]]
    logger.info(f"top_jobs: {len(result)} job posts")
    return result

def compute_top_posts(posts_df: pd.DataFrame, run_date: date, top_n: int = 10) -> pd.DataFrame:
    """
    Top 10 posts with the highest score
    """
    empty_df = pd.DataFrame(columns=["date", "rank", "post_id", "content_text", "score", "url", "post_type"])

    if posts_df.empty:
        return empty_df
    
    scored_df = posts_df[posts_df["score"].notna()].copy()

    if scored_df.empty:
        logger.warning("No posts with a score for top_posts metric")
        return empty_df
    
    scored_df["score"] = pd.to_numeric(scored_df["score"], errors="coerce")

    top = (
        scored_df
        .sort_values("score", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    top["rank"] = top.index + 1
    top["date"] = run_date

    result = top[["date", "rank", "post_id", "content_text", "score", "url", "post_type"]]
    logger.info(f"top_posts: {len(result)} posts")
    return result


def compute_data_quality_score(posts_df: pd.DataFrame, users_df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """
    KPI: Data Qaulity Score

    Calculated based on the columns that are tied to a platform, ignoring structuraly null columns that are unavoidable
    """
    rows = []

    # cols that have to have values for HN 
    posts_required = ["post_id", "author_username", "content_text", "created_at", "post_type"]

    # exclude the X columns
    users_required = ["user_id", "username", "platform", "karma_score", "created_at"]

    for table_name, df, required_cols in [
        ("posts", posts_df, posts_required),
        ("users", users_df, users_required)
    ]:
        if df.empty:
            quality_pct = 0.0
        else:
            existing_cols = [c for c in required_cols if c in df.columns]

            non_null_rows = df[existing_cols].notna().all(axis=1).sum()
            quality_pct = round((non_null_rows / len(df)) * 100, 2)
        
        rows.append({
            "date": run_date,
            "platform": PLATFORM_HN,
            "table_name": table_name,
            "quality_pct": quality_pct,
        })

    return pd.DataFrame(rows)

def run(bucket: str, run_date: date) -> dict[str, pd.DataFrame]:
    """
    Calculate all of the metrics and resturn a dict

    The keys are the names of the gold folders in S3
    Handler.py will use this dict for writing every DataFrame in S3
    """
    # load silver data
    posts_df = load_silver_posts(bucket, run_date)
    users_df = load_silver_users(bucket)

    # all metrics
    top_karma_high, top_karma_low = compute_top_karma_users(users_df, run_date)

    results = {
        "daily_posts_metric":    compute_daily_posts_metric(posts_df, run_date),
        "daily_users_metric":    compute_daily_users_metric(users_df, run_date),
        "top_karma_users_high":  top_karma_high,
        "top_karma_users_low":   top_karma_low,
        "top_jobs":              compute_top_jobs(posts_df, run_date),
        "top_posts":             compute_top_posts(posts_df, run_date),
        "data_quality_score":    compute_data_quality_score(posts_df, users_df, run_date),
    }

    for name, df in results.items():
        logger.info(f"  {name}: {len(df)} rows")
    
    return results
