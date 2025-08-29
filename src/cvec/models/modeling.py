from datetime import datetime
from enum import Enum
from typing import List

from pydantic import BaseModel


class TagSourceType(str, Enum):
    Sensor = "sensor_readings"
    Modeling = "modeling"


class BaseReadingModel(BaseModel):
    tag_id: int
    tag_value: float
    timestamp: float


class ModelingReadingModel(BaseReadingModel):
    pass


class BaseReadingsGroup(BaseModel):
    tag_id: int
    data: List[BaseReadingModel]
    source: TagSourceType


class ModelingReadingsGroup(BaseReadingsGroup):
    pass


class ModelingReadingsDataResponse(BaseModel):
    items: List[ModelingReadingsGroup]


class FetchModelingReadingsRequest(BaseModel):
    tag_ids: List[int]
    start_date: datetime
    end_date: datetime
    desired_points: int = 10000


class LatestReadingsRequest(BaseModel):
    tag_ids: List[int]


class LatestReadingsResponseItem(BaseModel):
    tag_id: int
    tag_value: float
    tag_value_changed_at: datetime


class LatestReadingsResponse(BaseModel):
    items: List[LatestReadingsResponseItem]
