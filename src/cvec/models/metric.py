from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class MetricDataPoint(BaseModel):
    """
    Represents a single data point for a metric.
    """

    name: str
    time: datetime
    value_double: Optional[float] = None
    value_string: Optional[str] = None

    model_config = ConfigDict(json_encoders={datetime: lambda dt: dt.isoformat()})


class Metric(BaseModel):
    """
    Represents metadata for a metric.
    """

    id: int
    name: str
    birth_at: Optional[datetime] = None
    death_at: Optional[datetime] = None

    model_config = ConfigDict(json_encoders={datetime: lambda dt: dt.isoformat()})
