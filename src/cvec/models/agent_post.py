from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class RecommendationType(str, Enum):
    """Type of recommendation."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AgentPostRecommendation(BaseModel):
    """
    Represents a recommendation for creating an agent post.
    """

    content: str = Field(..., min_length=1)
    recommendation_type: RecommendationType

class AgentPost(BaseModel):
    """
    Represents an agent post with optional recommendations.
    """

    author: str
    title: str
    content: Optional[str] = None
    image_id: Optional[UUID] = None
    recommendations: Optional[List[AgentPostRecommendation]] = None
