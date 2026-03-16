"""In-memory HTTP cache for GET requests with Cache-Control and ETag support."""

from dataclasses import dataclass
from typing import Any, Optional


MAX_CACHE_ENTRIES = 100


@dataclass
class CacheEntry:
    """A cached HTTP response."""

    data: Any
    etag: Optional[str]
    max_age: int
    stored_at: float

    @property
    def expires_at(self) -> float:
        """Monotonic time when this entry expires."""
        return self.stored_at + self.max_age


def parse_max_age(header: Optional[str]) -> Optional[int]:
    """Parse max-age value from a Cache-Control header.

    Returns:
        The max-age value in seconds, or None if not present.
    """
    if header is None:
        return None
    for directive in header.split(","):
        directive = directive.strip()
        if directive.startswith("max-age="):
            try:
                return int(directive[len("max-age=") :])
            except ValueError:
                return None
    return None
