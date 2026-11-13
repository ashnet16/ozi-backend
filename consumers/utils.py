import base64
import logging
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Farcaster timestamps count seconds from 2021-01-01 00:00:00 UTC, not from the Unix epoch.
FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)

# A Farcaster message hash is 20 bytes: 40 hex characters.
_HEX_HASH = re.compile(r"^[0-9a-fA-F]{40}$")


def farcaster_ts_to_datetime(farcaster_ts) -> datetime:
    try:
        return FARCASTER_EPOCH + timedelta(seconds=int(farcaster_ts))
    except Exception:
        logger.warning("Failed to parse Farcaster timestamp: %s", farcaster_ts)
        return datetime.now(timezone.utc)


def to_hex(value) -> str:
    """Return a hash as lowercase hex.

    Hashes reach the consumers in two encodings: the poller writes hex for the
    message hash and reaction target, while anything read from the raw protobuf
    dump (e.g. a parent cast hash) is base64.
    """
    if not value:
        return ""
    if isinstance(value, bytes):
        return value.hex()
    value = value.strip()
    if value.startswith(("0x", "0X")):
        value = value[2:]
    if _HEX_HASH.match(value):
        return value.lower()
    try:
        return base64.b64decode(value + "==").hex()
    except Exception:
        return ""
