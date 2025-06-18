from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel


class Span(BaseModel):
    """
    Represents a time span where a metric has a constant value.
    """

    id: Optional[Any] = None
    name: str
    value: Optional[Union[float, str]]
    raw_start_at: datetime
    raw_end_at: Optional[datetime]
    metadata: Optional[Any] = None
