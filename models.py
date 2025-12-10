from pydantic import BaseModel, field_validator
from datetime import date

# Use frozenset for O(1) lookup instead of O(n) list lookup
_ALLOWED_RESULTS = frozenset(['POS', 'NEG', 'N/A'])

class LabResult(BaseModel):
    sample_id: str
    test_date: date
    result: str
    viral_load: int

    @field_validator('result')
    def check_result_code(cls, v):
        if v not in _ALLOWED_RESULTS:
            raise ValueError(f"Invalid result code: '{v}'. Must be POS, NEG, or N/A")
        return v