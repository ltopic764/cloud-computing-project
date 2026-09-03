import logging
import os
from datetime import (
    datetime,
    timedelta,
    timezone,
    date,
)

import awswrangler as wr

from transformer import run


logging.getLogger().setLevel(
    logging.INFO
)

logger = logging.getLogger(
    __name__
)


S3_BUCKET = os.environ[
    "S3_BUCKET_NAME"
]

GOLD_PREFIX = "gold/twitter"


def get_yesterday() -> date:
    """
    Use the same logical processing date convention
    as the current Hacker News Gold Lambda.
    """

    return (
        datetime.now(timezone.utc)
        - timedelta(days=1)
    ).date()


def resolve_run_date(
    event: dict,
) -> date:
    """
    Manual test:
        {
            "run_date": "2020-07-25"
        }

    Normal scheduled execution:
        {}
    """

    value = event.get(
        "run_date"
    )

    if value is None:
        return get_yesterday()

    try:
        return datetime.strptime(
            value,
            "%Y-%m-%d",
        ).date()

    except (
        TypeError,
        ValueError,
    ) as e:
        raise ValueError(
            "run_date must use "
            "YYYY-MM-DD format"
        ) from e


def write_metric_to_s3(
    df,
    metric_name: str,
    partition_cols: list[str],
) -> None:
    """
    Write one Gold metric to S3.

    overwrite_partitions makes repeated execution
    for the same date idempotent.
    """

    if df.empty:
        logger.warning(
            f"{metric_name}: "
            "empty DataFrame, "
            "skipping write"
        )
        return

    path = (
        f"s3://{S3_BUCKET}/"
        f"{GOLD_PREFIX}/"
        f"{metric_name}/"
    )

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        partition_cols=partition_cols,
        mode="overwrite_partitions",
    )

    logger.info(
        f"{metric_name}: "
        f"written {len(df)} rows "
        f"to {path}"
    )


def handler(
    event: dict,
    context,
) -> dict:

    event = event or {}

    run_date = resolve_run_date(
        event
    )

    logger.info(
        "Twitter Gold Lambda "
        f"started for {run_date}"
    )

    metrics = run(
        S3_BUCKET,
        run_date,
    )

    write_config = {
        "daily_users_metric": [
            "platform",
            "date",
        ],

        "top_users_followers": [
            "date",
        ],

        "data_quality_score": [
            "platform",
            "date",
        ],
    }

    written = {}

    for (
        metric_name,
        partition_cols,
    ) in write_config.items():

        df = metrics.get(
            metric_name
        )

        if df is None:
            raise KeyError(
                "Transformer did not return "
                f"metric '{metric_name}'"
            )

        write_metric_to_s3(
            df,
            metric_name,
            partition_cols,
        )

        written[
            metric_name
        ] = len(df)

    return {
        "statusCode": 200,
        "run_date": str(
            run_date
        ),
        "metrics_written":
            written,
    }