import logging
import sys
import os
import time 
import requests

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared import to_utc_iso8601, clean_html, generate_user_id

import logging
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

PLATFORM = "HackerNews"
MAX_USERS_TO_FETCH = int(os.environ.get("MAX_USERS_TO_FETCH", "0")) or None

# every possible value for post_type that HN can have
VALID_POST_TYPES = {"story", "ask_hn", "comment", "job", "poll"}

def extract_post_type(tags: list) -> str:
    """
    Determine the post type based on the _tags field
    """
    if not tags or not isinstance(tags, list):
        return "unknown"
    
    # ask_hn check before story check
    # because these have both of the tags
    if "ask_hn" in tags:
        return "ask_hn"
    
    # the first element is always the type
    primary_type = tags[0] if tags else "unknown"

    return primary_type if primary_type in VALID_POST_TYPES else "unknown"

def extract_content_text(item: dict, post_type: str) -> str | None:
    """
    Get the posts text based on the type of post

    Every text is run through the clean_html()
    """
    if post_type == "ask_hn":
        title = item.get("title", "") or ""
        story_text = item.get("story_text", "") or ""

        clean_title = clean_html(title)
        clean_body = clean_html(story_text)

        parts = [p for p in [clean_title, clean_body] if p]
        return "\n\n".join(parts) if parts else None
    
    elif post_type == "comment":
        return clean_html(item.get("comment_text"))
    
    elif post_type in ("story", "job", "poll"):
        # only title
        return clean_html(item.get("title"))
    
    else:
        # unknown type
        return clean_html(item.get("title") or item.get("comment_text"))

def normalize_posts(items: list[dict]) -> pd.DataFrame:
    """
    Converts raw list HN item dicts into posts DataFrame
    """
    if not items:
        logger.warning("Empty item list, returning empty DataFrame")
        # return empty dataframe with correct columns
        return pd.DataFrame(columns=[
            "post_id", "author_username", "content_text",
            "created_at", "post_type", "score", "num_comments",
            "platform", "url"
        ])
    
    rows = []

    for item in items:
        # objectId is Algoglia ID
        post_id = item.get("objectID")

        # skip the item without ID
        if not post_id:
            logger.warning("Item without objectID, skipping")
            continue

        # get posts gype
        tags = item.get("_tags", [])
        post_type = extract_post_type(tags)

        logger.info(f"Item {post_id}: tags={tags}, post_type={post_type}")

        # get text
        content_text = extract_content_text(item, post_type)

        created_at = to_utc_iso8601(item.get("created_at"))

        rows.append({
            "post_id": str(post_id),
            "author_username": item.get("author"),
            "content_text": content_text,
            "created_at": created_at,
            "post_type": post_type,
            "score": item.get("points"),
            "num_comments": item.get("num_comments"),
            "platform": PLATFORM,
            "url": item.get("url"),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("All items skipped, DataFrame empty")            
        return df
        
    # remove dupllicates by post_id
    before = len(df)
    df = df.drop_duplicates(subset=["post_id"], keep="first")        
    after = len(df)

    if before != after:
        logger.info(f"Removed {before - after} duplicates from posts table")

    df["score"] = pd.array(df["score"], dtype="Int64")
    df["num_comments"] = pd.array(df["num_comments"], dtype="Int64")

    logger.info(f"normalize_posts: {len(df)} posts, types: {df['post_type'].value_counts().to_dict()}")
    return df
    
def fetch_hn_users(username: str, session: requests.Session) -> dict | None:
    """
    Fetch data user from firebase 

    Algolia search does not return info about the author only the post
    """
    url = f"https://hacker-news.firebaseio.com/v0/user/{username}.json"

    try:
        # timeout
        response = session.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        if data is None:
            logger.warning(f"HN user '{username}' does not exists or the account is deleted")
            return None
        
        return None
    
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching user '{username}'")
        return None
    
    except requests.exceptions.RequestsWarning as e:
        logger.warning(f"Error fetching user '{username}' : '{e}'")
        return None

def normalize_users(items: list[dict]) -> pd.DataFrame:
    """
    Get users from a list HN items and make DataFrame
    """
    if not items:
        return pd.DataFrame(columns=[
            "user_id", "username", "platform",
            "karma_score", "is_verified", "followers_count", "created_at"
        ])
    
    # collect unique authors
    unique_authors: dict[str, dict] = {}

    for item in items:
        author = item.get("author")

        if not author:
            continue

        # fallback
        if author not in unique_authors:
            unique_authors[author] = item.get("created_at")
    
    if not unique_authors:
        logger.warning("No authors found")
        return pd.DataFrame(columns=[
            "user_id", "username", "platform",
            "karma_score", "is_verified", "followers_count", "created_at"
        ])
    
    if MAX_USERS_TO_FETCH:
        unique_authors = dict(list(unique_authors.items())[:MAX_USERS_TO_FETCH])
        logger.info(f"Limited to {MAX_USERS_TO_FETCH} users")
        
    rows = []

    with requests.Session() as session:
        for i, (username, first_post_created_at) in enumerate(unique_authors.items()):
            # rate limiting
            if i > 0:
                time.sleep(0.2)

            user_data = fetch_hn_users(username, session)

            if user_data:
                # user found
                karma = user_data.get("karma")
                account_created = to_utc_iso8601(user_data.get("created"))
            else:
                karma = None
                account_created = to_utc_iso8601(first_post_created_at)
            
            rows.append({
                "user_id":         generate_user_id(username, PLATFORM),
                "username":        username,
                "platform":        PLATFORM,
                "karma_score":     karma,
                "is_verified":     None,
                "followers_count": None,
                "created_at":      account_created,
            })
    
    df = pd.DataFrame(rows)

    df = df.drop_duplicates(subset=["username"], keep="first")
    df["karma_score"] = pd.array(df["karma_score"], dtype="Int64")

    logger.info(f"normalize_users: {len(df)} unique HN users")
    return df

def normalize(items: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    posts_df = normalize_posts(items)
    users_df = normalize_users(items)

    return posts_df, users_df
