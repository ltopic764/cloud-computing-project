#!/usr/bin/env python3
"""
Local end-to-end test for the Twitter/X Bronze -> Silver -> Gold pipeline.

IMPORTANT:
- Does NOT connect to AWS.
- Does NOT read/write S3.
- Does NOT invoke Lambda, Step Functions, EC2, PostgreSQL, etc.
- Therefore it cannot create AWS costs.

What it tests:
1. Bronze logic: CSV rows are wrapped in the same raw JSON-like structure used by Bronze.
2. Silver logic: imports and executes YOUR real src/silver/twitter/normalizer.py.
3. Silver cross-file behavior: simulates the handler's global post deduplication and X user merge.
4. Gold logic: if src/gold/twitter/transformer.py exists, imports YOUR real compute_* functions
   and tests daily_users_metric, top users by followers, and data quality.
5. Optional synthetic 2026 dataset for testing new_users > 0.

Run from project root, for example:

    python tests/test_twitter_pipeline_local.py ^
        covid19_tweets_sample_5000.csv ^
        covid19_tweets_sample_5000_10000.csv ^
        covid19_tweets_sample_10k_15k.csv

or PowerShell on one line:

    python tests/test_twitter_pipeline_local.py covid19_tweets_sample_5000.csv covid19_tweets_sample_5000_10000.csv covid19_tweets_sample_10k_15k.csv

Generate a small 2026 demo file:

    python tests/test_twitter_pipeline_local.py --make-demo

Then test it:

    python tests/test_twitter_pipeline_local.py twitter_demo_2026.csv --run-date 2026-09-04
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


POST_COLUMNS = [
    "post_id",
    "author_username",
    "content_text",
    "created_at",
    "post_type",
    "score",
    "num_comments",
    "platform",
    "url",
]

USER_COLUMNS = [
    "user_id",
    "username",
    "platform",
    "karma_score",
    "is_verified",
    "followers_count",
    "created_at",
]


def banner(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def ok(message: str) -> None:
    print(f"[OK]   {message}")


def info(message: str) -> None:
    print(f"[INFO] {message}")


def warn(message: str) -> None:
    print(f"[WARN] {message}")


def fail(message: str) -> None:
    raise AssertionError(message)


def find_project_root(start: Path) -> Path:
    """
    Find the repository root by looking for src/silver/twitter/normalizer.py.
    """
    candidates = [start.resolve(), *start.resolve().parents]

    for candidate in candidates:
        if (candidate / "src" / "silver" / "twitter" / "normalizer.py").exists():
            return candidate

    raise FileNotFoundError(
        "Cannot find project root. Run this script from inside the repository "
        "or place it under <project>/tests/."
    )


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)

    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_real_silver_normalizer(project_root: Path):
    """
    Load YOUR actual Twitter Silver normalizer.

    The normalizer itself imports ../shared.py, so we temporarily add
    src/silver to sys.path just like the Lambda folder layout expects.
    """
    silver_root = project_root / "src" / "silver"
    twitter_dir = silver_root / "twitter"

    sys.path.insert(0, str(silver_root))
    sys.path.insert(0, str(twitter_dir))

    try:
        return load_module(
            "twitter_silver_normalizer_local_test",
            twitter_dir / "normalizer.py",
        )
    finally:
        # Keep loaded modules in sys.modules, but restore normal search order.
        for value in (str(twitter_dir), str(silver_root)):
            try:
                sys.path.remove(value)
            except ValueError:
                pass


def csv_to_bronze_items(csv_path: Path) -> list[dict]:
    """
    Local equivalent of the core Twitter Bronze transformation.

    The real Lambda reads CSV from S3 and wraps every DictReader row like:
        {
            "source": "X",
            "source_format": "csv",
            "source_s3_key": ...,
            "ingested_at": ...,
            "raw": row
        }

    We do the same locally, without boto3/S3.
    """
    items: list[dict] = []

    with csv_path.open("r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)

        if not reader.fieldnames:
            fail(f"{csv_path.name}: CSV has no header")

        for row in reader:
            items.append(
                {
                    "source": "X",
                    "source_format": "csv",
                    "source_s3_key": f"input/twitter/{csv_path.name}",
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "raw": row,
                }
            )

    return items


def validate_bronze(items: list[dict], csv_path: Path) -> None:
    assert items, f"{csv_path.name}: no Bronze rows created"

    required_wrapper_fields = {
        "source",
        "source_format",
        "source_s3_key",
        "ingested_at",
        "raw",
    }

    for index, item in enumerate(items[:10]):
        missing = required_wrapper_fields - set(item.keys())
        assert not missing, (
            f"{csv_path.name}: Bronze row {index} missing wrapper fields: {missing}"
        )
        assert item["source"] == "X"
        assert item["source_format"] == "csv"
        assert isinstance(item["raw"], dict)

    ok(f"{csv_path.name}: Bronze wrapper valid for {len(items)} rows")


def validate_silver_schema(posts_df: pd.DataFrame, users_df: pd.DataFrame, label: str) -> None:
    missing_posts = set(POST_COLUMNS) - set(posts_df.columns)
    missing_users = set(USER_COLUMNS) - set(users_df.columns)

    assert not missing_posts, f"{label}: missing Silver post columns: {sorted(missing_posts)}"
    assert not missing_users, f"{label}: missing Silver user columns: {sorted(missing_users)}"

    if not posts_df.empty:
        assert posts_df["post_id"].notna().all(), f"{label}: null post_id found"
        assert posts_df["post_id"].is_unique, f"{label}: duplicate post_id inside one batch"
        assert (posts_df["platform"] == "X").all(), f"{label}: non-X post found"
        assert posts_df["post_type"].isin(["tweet", "retweet"]).all(), (
            f"{label}: unexpected post_type"
        )

        parsed = pd.to_datetime(posts_df["created_at"], utc=True, errors="coerce")
        invalid_dates = int(parsed.isna().sum())
        if invalid_dates:
            warn(f"{label}: {invalid_dates} posts have invalid/null created_at")
        else:
            ok(f"{label}: all post created_at values parse as UTC timestamps")

    if not users_df.empty:
        assert (users_df["platform"] == "X").all(), f"{label}: non-X user found"

        named_users = users_df[users_df["username"].notna()]
        assert not named_users["username"].duplicated().any(), (
            f"{label}: duplicate username inside one normalized batch"
        )

        if "followers_count" in users_df.columns:
            bad_followers = pd.to_numeric(
                users_df["followers_count"], errors="coerce"
            ).isna() & users_df["followers_count"].notna()

            assert not bad_followers.any(), (
                f"{label}: followers_count contains non-numeric values after Silver normalization"
            )

    ok(f"{label}: Silver schemas and basic constraints are valid")


def merge_users_like_silver_handler(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Local simulation of src/silver/twitter/handler.py -> merge_x_users().

    This deliberately mirrors your handler:
    - concat existing + new
    - drop exact duplicates, keep last
    - for named users keep one row per (username, platform), keep last
    """
    if existing_df.empty:
        combined = new_df.copy()
    elif new_df.empty:
        combined = existing_df.copy()
    else:
        combined = pd.concat([existing_df, new_df], ignore_index=True)

    if combined.empty:
        return combined

    combined = combined.drop_duplicates(keep="last")

    if "username" in combined.columns:
        users_with_username = combined[
            combined["username"].notna()
        ].drop_duplicates(
            subset=["username", "platform"],
            keep="last",
        )

        users_without_username = combined[
            combined["username"].isna()
        ]

        combined = pd.concat(
            [users_with_username, users_without_username],
            ignore_index=True,
        )

    return combined


def merge_posts_like_silver_handler(
    existing_posts: pd.DataFrame,
    new_posts: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """
    Local simulation of global post deduplication in the Silver handler.

    The real handler reads all existing post_id values from S3 and removes
    matching posts from the next batch before append.
    """
    if existing_posts.empty:
        return new_posts.copy(), 0

    existing_ids = set(
        existing_posts["post_id"].dropna().astype(str).tolist()
    )

    before = len(new_posts)

    deduplicated = new_posts[
        ~new_posts["post_id"].astype("string").isin(existing_ids)
    ].copy()

    removed = before - len(deduplicated)

    combined = pd.concat(
        [existing_posts, deduplicated],
        ignore_index=True,
    )

    return combined, removed


def process_csv_files(csv_files: Iterable[Path], normalizer):
    global_posts = pd.DataFrame(columns=POST_COLUMNS)
    global_users = pd.DataFrame(columns=USER_COLUMNS)

    total_csv_rows = 0
    total_cross_file_duplicates = 0

    per_file_results = []

    for csv_path in csv_files:
        banner(f"FILE: {csv_path.name}")

        bronze_items = csv_to_bronze_items(csv_path)
        validate_bronze(bronze_items, csv_path)

        total_csv_rows += len(bronze_items)

        # THIS CALL EXECUTES YOUR REAL SILVER NORMALIZER.
        posts_df, users_df = normalizer.normalize(bronze_items)

        validate_silver_schema(posts_df, users_df, csv_path.name)

        before_global_posts = len(global_posts)

        global_posts, removed = merge_posts_like_silver_handler(
            global_posts,
            posts_df,
        )

        global_users = merge_users_like_silver_handler(
            global_users,
            users_df,
        )

        total_cross_file_duplicates += removed

        info(
            f"{csv_path.name}: input={len(bronze_items)}, "
            f"normalized posts={len(posts_df)}, "
            f"batch users={len(users_df)}, "
            f"cross-file duplicate posts removed={removed}"
        )

        info(
            f"Silver snapshot after file: "
            f"posts {before_global_posts} -> {len(global_posts)}, "
            f"users={len(global_users)}"
        )

        per_file_results.append(
            {
                "file": csv_path.name,
                "input_rows": len(bronze_items),
                "normalized_posts": len(posts_df),
                "batch_users": len(users_df),
                "cross_file_duplicates_removed": removed,
            }
        )

    return {
        "posts": global_posts.reset_index(drop=True),
        "users": global_users.reset_index(drop=True),
        "total_csv_rows": total_csv_rows,
        "cross_file_duplicates_removed": total_cross_file_duplicates,
        "per_file": per_file_results,
    }


def load_real_gold_transformer(project_root: Path):
    gold_transformer = (
        project_root / "src" / "gold" / "twitter" / "transformer.py"
    )

    if not gold_transformer.exists():
        return None

    try:
        return load_module(
            "twitter_gold_transformer_local_test",
            gold_transformer,
        )
    except ModuleNotFoundError as exc:
        warn(
            "Twitter Gold transformer exists, but one of its imports is missing locally: "
            f"{exc}. Silver test will still continue."
        )
        return None


def test_gold(
    gold,
    posts_df: pd.DataFrame,
    users_df: pd.DataFrame,
    run_date: date,
) -> None:
    banner(f"GOLD TEST FOR run_date={run_date}")

    required_functions = [
        "compute_daily_users_metric",
        "compute_top_users_by_followers",
    ]

    missing = [name for name in required_functions if not hasattr(gold, name)]

    if missing:
        fail(
            "Gold transformer exists but is missing required functions: "
            + ", ".join(missing)
        )

    # These are YOUR real Gold functions.
    daily = gold.compute_daily_users_metric(users_df.copy(), run_date)
    top = gold.compute_top_users_by_followers(users_df.copy(), run_date)

    assert len(daily) == 1, "daily_users_metric must contain exactly one row"

    daily_row = daily.iloc[0]

    expected_total_users = int(
        users_df["username"].dropna().astype("string").nunique()
    )

    assert int(daily_row["total_users"]) == expected_total_users, (
        f"Gold total_users mismatch: got {daily_row['total_users']}, "
        f"expected {expected_total_users}"
    )

    parsed_user_dates = pd.to_datetime(
        users_df["created_at"],
        utc=True,
        errors="coerce",
    )

    expected_new_users = int(
        (parsed_user_dates.dt.date == run_date).sum()
    )

    assert int(daily_row["new_users"]) == expected_new_users, (
        f"Gold new_users mismatch: got {daily_row['new_users']}, "
        f"expected {expected_new_users}"
    )

    assert len(top) <= 10, "Top followers metric contains more than 10 rows"

    if len(top) > 1:
        followers = pd.to_numeric(
            top["followers_count"],
            errors="coerce",
        )
        assert followers.is_monotonic_decreasing, (
            "Top followers result is not sorted descending"
        )

    if not top.empty:
        assert list(top["rank"]) == list(range(1, len(top) + 1)), (
            "Gold ranks must be 1..N"
        )

    ok(
        f"daily_users_metric: total_users={int(daily_row['total_users'])}, "
        f"new_users={int(daily_row['new_users'])}"
    )

    if not top.empty:
        first = top.iloc[0]
        ok(
            "top_users_followers: "
            f"#1 {first['username']} with {int(first['followers_count'])} followers"
        )

    if hasattr(gold, "compute_data_quality_score"):
        quality = gold.compute_data_quality_score(
            posts_df.copy(),
            users_df.copy(),
            run_date,
        )
        info("Data Quality Score:")
        print(quality.to_string(index=False))

    print()
    print("Gold daily metric:")
    print(daily.to_string(index=False))

    print()
    print("Gold top followers:")
    print(top.to_string(index=False))


def make_demo_csv(output_path: Path) -> None:
    fields = [
        "user_name",
        "user_location",
        "user_description",
        "user_created",
        "user_followers",
        "user_friends",
        "user_favourites",
        "user_verified",
        "date",
        "text",
        "hashtags",
        "source",
        "is_retweet",
    ]

    rows = [
        {
            "user_name": "demo_old_user",
            "user_location": "Serbia",
            "user_description": "Old demo account",
            "user_created": "2020-01-10 10:00:00",
            "user_followers": "15000",
            "user_friends": "100",
            "user_favourites": "50",
            "user_verified": "True",
            "date": "2026-09-02 10:00:00",
            "text": "Testing our cloud project on September 2",
            "hashtags": "['cloud']",
            "source": "Web App",
            "is_retweet": "False",
        },
        {
            "user_name": "demo_sep02",
            "user_location": "Serbia",
            "user_description": "New user September 2",
            "user_created": "2026-09-02 08:15:00",
            "user_followers": "250",
            "user_friends": "20",
            "user_favourites": "4",
            "user_verified": "False",
            "date": "2026-09-02 11:00:00",
            "text": "My first demo tweet",
            "hashtags": "['demo']",
            "source": "Web App",
            "is_retweet": "False",
        },
        {
            "user_name": "demo_sep03_a",
            "user_location": "Serbia",
            "user_description": "New user September 3",
            "user_created": "2026-09-03 09:00:00",
            "user_followers": "850",
            "user_friends": "30",
            "user_favourites": "12",
            "user_verified": "False",
            "date": "2026-09-03 09:30:00",
            "text": "Gold layer testing",
            "hashtags": "['aws']",
            "source": "Web App",
            "is_retweet": "False",
        },
        {
            "user_name": "demo_sep03_b",
            "user_location": "Germany",
            "user_description": "Another new user",
            "user_created": "2026-09-03 12:00:00",
            "user_followers": "2500",
            "user_friends": "120",
            "user_favourites": "30",
            "user_verified": "True",
            "date": "2026-09-03 13:00:00",
            "text": "Testing Twitter metrics",
            "hashtags": "['metrics']",
            "source": "Web App",
            "is_retweet": "False",
        },
        {
            "user_name": "demo_sep04",
            "user_location": "Serbia",
            "user_description": "Defense demo account",
            "user_created": "2026-09-04 08:00:00",
            "user_followers": "450",
            "user_friends": "40",
            "user_favourites": "8",
            "user_verified": "False",
            "date": "2026-09-04 09:00:00",
            "text": "Preparing for project defense",
            "hashtags": "['cloud']",
            "source": "Web App",
            "is_retweet": "False",
        },
        {
            "user_name": "demo_old_user",
            "user_location": "Serbia",
            "user_description": "Old demo account",
            "user_created": "2020-01-10 10:00:00",
            "user_followers": "15025",
            "user_friends": "100",
            "user_favourites": "51",
            "user_verified": "True",
            "date": "2026-09-04 12:00:00",
            "text": "Another tweet from existing user",
            "hashtags": "['demo']",
            "source": "Web App",
            "is_retweet": "False",
        },
        {
            "user_name": "demo_sep04",
            "user_location": "Serbia",
            "user_description": "Defense demo account",
            "user_created": "2026-09-04 08:00:00",
            "user_followers": "500",
            "user_friends": "42",
            "user_favourites": "9",
            "user_verified": "False",
            "date": "2026-09-04 14:00:00",
            "text": "RT @demo_old_user Testing retweets",
            "hashtags": "['aws']",
            "source": "Web App",
            "is_retweet": "True",
        },
    ]

    with output_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    ok(f"Created demo dataset: {output_path}")
    info("Expected: 7 posts, 5 unique users, 1 retweet")
    info("Expected new_users: 2026-09-02 -> 1, 2026-09-03 -> 2, 2026-09-04 -> 1")


def run_demo_specific_assertions(
    csv_files: list[Path],
    posts_df: pd.DataFrame,
    users_df: pd.DataFrame,
) -> None:
    if len(csv_files) != 1 or csv_files[0].name != "twitter_demo_2026.csv":
        return

    banner("DEMO DATASET ASSERTIONS")

    assert len(posts_df) == 7, f"Demo expected 7 posts, got {len(posts_df)}"

    named_users = users_df[users_df["username"].notna()]
    assert named_users["username"].nunique() == 5, (
        f"Demo expected 5 unique users, got {named_users['username'].nunique()}"
    )

    assert int((posts_df["post_type"] == "retweet").sum()) == 1, (
        "Demo expected exactly 1 retweet"
    )

    demo_sep04 = users_df[users_df["username"] == "demo_sep04"]
    assert len(demo_sep04) == 1, "demo_sep04 must appear exactly once in Silver users"
    assert int(demo_sep04.iloc[0]["followers_count"]) == 500, (
        "Silver keep='last' user update failed: demo_sep04 should have 500 followers"
    )

    ok("Demo dataset: 7 posts")
    ok("Demo dataset: 5 unique users")
    ok("Demo dataset: exactly 1 retweet")
    ok("Demo dataset: latest demo_sep04 follower count (500) preserved")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Local Twitter Bronze -> Silver -> Gold pipeline test"
    )

    parser.add_argument(
        "csv_files",
        nargs="*",
        help="CSV files to process, in the same order as Bronze batches",
    )

    parser.add_argument(
        "--run-date",
        default="2026-09-04",
        help="Gold metric date in YYYY-MM-DD format (default: 2026-09-04)",
    )

    parser.add_argument(
        "--make-demo",
        action="store_true",
        help="Create twitter_demo_2026.csv in the current directory",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.make_demo:
        make_demo_csv(Path.cwd() / "twitter_demo_2026.csv")

        if not args.csv_files:
            return 0

    if not args.csv_files:
        print(
            "No CSV files supplied.\n\n"
            "Example:\n"
            "  python tests/test_twitter_pipeline_local.py "
            "covid19_tweets_sample_5000.csv "
            "covid19_tweets_sample_5000_10000.csv "
            "covid19_tweets_sample_10k_15k.csv\n\n"
            "Or create demo data:\n"
            "  python tests/test_twitter_pipeline_local.py --make-demo"
        )
        return 2

    script_location = Path(__file__).resolve()
    project_root = find_project_root(script_location.parent)

    banner("LOCAL TWITTER PIPELINE TEST")
    info(f"Project root: {project_root}")
    info("AWS access: NONE")
    info("S3 writes: NONE")
    info("Lambda invocations: NONE")
    info("AWS cost from this script: NONE")

    csv_files = [Path(value).resolve() for value in args.csv_files]

    missing_files = [str(path) for path in csv_files if not path.exists()]
    if missing_files:
        raise FileNotFoundError(
            "CSV file(s) not found:\n  " + "\n  ".join(missing_files)
        )

    run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date()

    normalizer = load_real_silver_normalizer(project_root)

    result = process_csv_files(csv_files, normalizer)

    posts_df = result["posts"]
    users_df = result["users"]

    banner("SILVER FINAL SUMMARY")

    print(f"Total CSV/Bronze rows:             {result['total_csv_rows']}")
    print(f"Cross-file duplicate posts:       {result['cross_file_duplicates_removed']}")
    print(f"Final unique Silver posts:        {len(posts_df)}")
    print(
        "Final unique named X users:       "
        f"{users_df['username'].dropna().astype('string').nunique()}"
    )
    print(f"Final Silver user rows:           {len(users_df)}")

    if not posts_df.empty:
        print()
        print("Post types:")
        print(posts_df["post_type"].value_counts(dropna=False).to_string())

    if not users_df.empty and "followers_count" in users_df.columns:
        top_local = (
            users_df[
                users_df["username"].notna()
                & users_df["followers_count"].notna()
            ]
            .sort_values("followers_count", ascending=False)
            .head(10)[["username", "followers_count", "is_verified"]]
        )

        print()
        print("Top 10 followers from final Silver snapshot:")
        print(top_local.to_string(index=False))

    assert posts_df["post_id"].is_unique, "Final Silver posts are not globally unique"

    named = users_df[users_df["username"].notna()]
    assert not named.duplicated(subset=["username", "platform"]).any(), (
        "Final Silver users contain duplicate (username, platform)"
    )

    ok("Final Silver posts are globally unique by post_id")
    ok("Final Silver named users are unique by (username, platform)")

    run_demo_specific_assertions(csv_files, posts_df, users_df)

    gold = load_real_gold_transformer(project_root)

    if gold is None:
        banner("GOLD")
        warn(
            "src/gold/twitter/transformer.py was not found or could not be imported. "
            "Bronze + Silver tests PASSED; Gold test was SKIPPED."
        )
    else:
        test_gold(gold, posts_df, users_df, run_date)

    banner("RESULT")
    print("ALL EXECUTED LOCAL TESTS PASSED.")
    print("No AWS resources were touched.")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print()
        print("[FAILED]", exc)
        raise SystemExit(1)
    except Exception as exc:
        print()
        print(f"[ERROR] {type(exc).__name__}: {exc}")
        raise
