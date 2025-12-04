from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class Severity(str, Enum):
    """Severity level for recommendations and tags."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AgentPostRecommendation(BaseModel):
    """
    Represents a recommendation for creating an agent post.
    """

    content: str = Field(..., min_length=1)
    severity: Severity


class AgentPostTag(BaseModel):
    """
    Represents a tag for creating an agent post.
    """

    content: str = Field(..., min_length=1)
    severity: Severity


class AgentPost(BaseModel):
    """
    Represents an agent post with optional recommendations and tags.
    """

    author: str
    title: str
    content: Optional[str] = None
    image_id: Optional[str] = None
    recommendations: Optional[List[AgentPostRecommendation]] = None
    tags: Optional[List[AgentPostTag]] = None
