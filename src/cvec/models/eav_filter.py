from typing import Optional, Union

from pydantic import BaseModel


class EAVFilter(BaseModel):
    """
    Represents a filter for querying EAV data.

    Filters are used to narrow down results based on column values:
    - Use numeric_min/numeric_max for numeric range filtering (min inclusive, max exclusive)
    - Use string_value for exact string matching
    - Use boolean_value for boolean matching
    """

    column_name: str
    numeric_min: Optional[Union[int, float]] = None
    numeric_max: Optional[Union[int, float]] = None
    string_value: Optional[str] = None
    boolean_value: Optional[bool] = None
