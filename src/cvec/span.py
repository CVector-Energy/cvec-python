from datetime import datetime
from typing import Any, Optional, Union


class Span:
    """
    Represents a time span where a tag has a constant value.
    """

    def __init__(
        self,
        id: Optional[Any],
        tag_name: str,
        value: Optional[Union[float, str]],
        start_at: datetime,
        end_at: Optional[datetime],
        raw_start_at: datetime,
        raw_end_at: Optional[datetime],
        metadata: Optional[Any],
    ):
        self.id = id
        self.tag_name = tag_name
        self.value = value
        self.start_at = start_at
        self.end_at = end_at
        self.raw_start_at = raw_start_at
        self.raw_end_at = raw_end_at
        self.metadata = metadata

    def __repr__(self) -> str:
        return (
            f"Span(id={self.id!r}, tag_name={self.tag_name!r}, value={self.value!r}, "
            f"start_at={self.start_at!r}, end_at={self.end_at!r}, "
            f"raw_start_at={self.raw_start_at!r}, raw_end_at={self.raw_end_at!r}, "
            f"metadata={self.metadata!r})"
        )
