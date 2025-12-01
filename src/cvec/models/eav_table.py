from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class EAVTable(BaseModel):
    """
    Represents an EAV table metadata record.
    """

    id: str
    tenant_id: int
    name: str
    continuation_token: Optional[str] = None
    last_sync_at: Optional[datetime] = None
    total_rows_synced: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_etag: Optional[str] = None

    model_config = ConfigDict(json_encoders={datetime: lambda dt: dt.isoformat()})
