from datetime import datetime
from typing import Optional


class Metric:
    """
    Represents metadata for a metric.
    """

    id: int
    name: str
    birth_at: Optional[datetime]
    death_at: Optional[datetime]

    def __init__(
        self,
        id: int,
        name: str,
        birth_at: Optional[datetime],
        death_at: Optional[datetime],
    ):
        self.id = id
        self.name = name
        self.birth_at = birth_at
        self.death_at = death_at

    def __repr__(self) -> str:
        return (
            f"Metric(id={self.id!r}, name={self.name!r}, "
            f"birth_at={self.birth_at!r}, death_at={self.death_at!r})"
        )
