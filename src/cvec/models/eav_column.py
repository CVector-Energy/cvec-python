from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class EAVColumn(BaseModel):
    """
    Represents an EAV column metadata record.
    """

    eav_table_id: str
    eav_column_id: str
    name: str
    type: str
    created_at: Optional[datetime] = None

    model_config = ConfigDict(json_encoders={datetime: lambda dt: dt.isoformat()})
