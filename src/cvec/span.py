from datetime import datetime
from typing import Any, Optional, Union


class Span:
    """
    Represents a time span where a metric has a constant value.
    """

    id: Optional[Any]
    name: str
    value: Optional[Union[float, str]]
    raw_start_at: datetime
    raw_end_at: Optional[datetime]
    metadata: Optional[Any]

    def __init__(
        self,
        id: Optional[Any],
        name: str,
        value: Optional[Union[float, str]],
        raw_start_at: datetime,
        raw_end_at: Optional[datetime],
        metadata: Optional[Any],
    ):
        self.id = id
        self.name = name
        self.value = value
        self.raw_start_at = raw_start_at
        self.raw_end_at = raw_end_at
        self.metadata = metadata

    def __repr__(self) -> str:
        return (
            f"Span(id={self.id!r}, name={self.name!r}, value={self.value!r}, "
            f"raw_start_at={self.raw_start_at!r}, raw_end_at={self.raw_end_at!r}, "
            f"metadata={self.metadata!r})"
        )
