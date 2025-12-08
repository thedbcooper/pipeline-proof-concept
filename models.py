from pydantic import BaseModel, field_validator
from datetime import date

class LabResult(BaseModel):
    sample_id: str
    test_date: date
    result: str
    viral_load: int

    @field_validator('result')
    def check_result_code(cls, v):
        allowed = ['POS', 'NEG', 'N/A']
        if v not in allowed:
            raise ValueError(f"Invalid result code: '{v}'. Must be POS, NEG, or N/A")
        return v