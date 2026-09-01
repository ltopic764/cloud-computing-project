import hashlib
import logging
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared import to_utc_iso8601, generate_user_id


logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

PLATFORM = "X"


def empty_to_none(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, str):
        value = value.strip()

        if not value:
            return None

        if value.lower() in {"null", "none", "nan", "nat"}:
            return None

    return value


def parse_nullable_bool(value) -> bool | None:
    value = empty_to_none(value)

    if value is None:
        return None

    if isinstance(value, bool):
        return value

    value = str(value).strip().lower()

    if value in {"true", "1", "yes"}:
        return True

    if value in {"false", "0", "no"}:
        return False

    return None


def parse_nullable_int(value) -> int | None:
    value = empty_to_none(value)

    if value is None:
        return None

    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def generate_post_id(
    username: str | None,
    created_at: str | None,
    text: str | None,
) -> str:
    """
    X dataset nema pouzdan originalni post ID,
    pa koristimo hash(username + date + text).
    """
    value = (
        f"{username or ''}|"
        f"{created_at or ''}|"
        f"{text or ''}"
    )

    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def extract_post_type(item: dict, text: str | None) -> str:
    """
    Tweet ili retweet.
    """
    is_retweet = parse_nullable_bool(
        item.get("is_retweet")
    )

    if is_retweet is True:
        return "retweet"

    if text and text.lstrip().startswith("RT @"):
        return "retweet"

    return "tweet"


def normalize_posts(items: list[dict]) -> pd.DataFrame:
    """
    Convert raw Twitter/X Bronze records into posts DataFrame.
    """
    if not items:
        return pd.DataFrame(columns=[
            "post_id",
            "author_username",
            "content_text",
            "created_at",
            "post_type",
            "score",
            "num_comments",
            "platform",
            "url",
        ])

    rows = []

    for item in items:
        raw = item.get("raw", item)

        if not isinstance(raw, dict):
            logger.warning("Invalid Twitter Bronze item, skipping")
            continue

        username = empty_to_none(
            raw.get("user_name")
        )

        text = empty_to_none(
            raw.get("text")
        )

        created_at = to_utc_iso8601(
            empty_to_none(raw.get("date"))
        )

        post_id = generate_post_id(
            username,
            created_at,
            text,
        )

        post_type = extract_post_type(
            raw,
            text,
        )

        rows.append({
            "post_id": post_id,
            "author_username": username,
            "content_text": text,
            "created_at": created_at,
            "post_type": post_type,

            # HN-only fields
            "score": None,
            "num_comments": None,

            "platform": PLATFORM,

            # Exists in the common HN posts schema
            "url": None,
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Remove duplicates inside current batch
    before = len(df)

    df = df.drop_duplicates(
        subset=["post_id"],
        keep="first",
    )

    if before != len(df):
        logger.info(
            f"Removed {before - len(df)} duplicate X posts"
        )

    # Keep the same nullable integer types as HackerNews
    df["score"] = pd.array(
        df["score"],
        dtype="Int64",
    )

    df["num_comments"] = pd.array(
        df["num_comments"],
        dtype="Int64",
    )

    logger.info(
        f"normalize_posts: {len(df)} X posts"
    )

    return df


def normalize_users(items: list[dict]) -> pd.DataFrame:
    """
    Convert raw Twitter/X Bronze records into users DataFrame.
    """
    if not items:
        return pd.DataFrame(columns=[
            "user_id",
            "username",
            "platform",
            "karma_score",
            "is_verified",
            "followers_count",
            "created_at",
        ])

    rows = []

    for item in items:
        raw = item.get("raw", item)

        if not isinstance(raw, dict):
            logger.warning("Invalid Twitter Bronze item, skipping")
            continue

        username = empty_to_none(
            raw.get("user_name")
        )

        if username is not None:
            username = str(username)

        user_id = (
            generate_user_id(username, PLATFORM)
            if username
            else None
        )

        rows.append({
            "user_id": user_id,
            "username": username,
            "platform": PLATFORM,

            # HN-only field
            "karma_score": None,

            "is_verified": parse_nullable_bool(
                raw.get("user_verified")
            ),

            "followers_count": parse_nullable_int(
                raw.get("user_followers")
            ),

            "created_at": to_utc_iso8601(
                empty_to_none(
                    raw.get("user_created")
                )
            ),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # One row per X username
    users_with_username = df[
        df["username"].notna()
    ].drop_duplicates(
        subset=["username"],
        keep="last",
    )

    # Preserve rows even if username is null
    users_without_username = df[
        df["username"].isna()
    ]

    df = pd.concat(
        [
            users_with_username,
            users_without_username,
        ],
        ignore_index=True,
    )

    # Same nullable types as the shared schema
    df["karma_score"] = pd.array(
        df["karma_score"],
        dtype="Int64",
    )

    df["is_verified"] = pd.array(
        df["is_verified"],
        dtype="boolean",
    )

    df["followers_count"] = pd.array(
        df["followers_count"],
        dtype="Int64",
    )

    logger.info(
        f"normalize_users: {len(df)} unique X users"
    )

    return df


def normalize(
    items: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:

    posts_df = normalize_posts(items)
    users_df = normalize_users(items)

    return posts_df, users_df