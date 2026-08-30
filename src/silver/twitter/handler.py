import logging

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def handler(event: dict, context) -> dict:
    """
    Twitter Silver Lambda — placeholder.
    """
    return {
        "statusCode": 200,
        "message": "Placeholder",
    }
