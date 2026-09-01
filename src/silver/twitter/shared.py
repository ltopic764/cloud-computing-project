import uuid
import logging
from datetime import datetime, timezone

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def to_utc_iso8601(value) -> str | None:
    """
    Convert time to UTC ISO-8601 string format
    
    Accepts both Integer and String values depending on the format in which the data is received from HN and Twitter

    Returns None if the values in invalid or None
    """
    if value is None:
        return None
    
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
            return dt.isoformat()
        
        if isinstance(value, str):
            value = value.strip()

            if not value:
                return None
            
            # try and parse the string
            dt = datetime.fromisoformat(value)

            # if string has no timezone info, say its UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                # if has timezone info, convert to UTC
                dt = dt.astimezone(timezone.utc)
            
            return dt.isoformat()
    except (ValueError, TypeError, OSError) as e:
        logger.warning(f"Cannot convert time '{value}': '{e}'")
        return None
    
    return None

def clean_html(text: str | None) -> str | None:
    """
    Remove HTML tags from text
    """
    if text is None:
        return None
    
    if not isinstance(text, str):
        return None
    
    text = text.strip()

    if not text:
        return None
    
    soup = BeautifulSoup(text, "html.parser")
    clean = soup.get_text(separator=" ")

    clean = " ".join(clean.split())

    return clean if clean else None

def generate_user_id(username: str, platform: str) -> str:
    """
    Generate UUID for a user based on username + platform
    """
    # concatenate username and platform into string
    name = f"{username.lower().strip()}:{platform}"
    # uuid5
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))
