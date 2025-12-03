from typing import Optional, Union

from pydantic import BaseModel, model_validator


class EAVFilter(BaseModel):
    """
    Represents a filter for querying EAV data.

    Filters are used to narrow down results based on column values:
    - Use column_name with select_from_eav() for human-readable column names
    - Use column_id with select_from_eav_id() for direct column IDs
    - Use numeric_min/numeric_max for numeric range filtering (min inclusive, max exclusive)
    - Use string_value for exact string matching
    - Use boolean_value for boolean matching

    Exactly one of column_name or column_id must be provided.
    """

    column_name: Optional[str] = None
    column_id: Optional[str] = None
    numeric_min: Optional[Union[int, float]] = None
    numeric_max: Optional[Union[int, float]] = None
    string_value: Optional[str] = None
    boolean_value: Optional[bool] = None

    @model_validator(mode="after")
    def check_column_identifier(self) -> "EAVFilter":
        if self.column_name is None and self.column_id is None:
            raise ValueError("Either column_name or column_id must be provided")
        if self.column_name is not None and self.column_id is not None:
            raise ValueError("Only one of column_name or column_id should be provided")
        return self
