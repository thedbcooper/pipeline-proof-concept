from pydantic import BaseModel, field_validator
from datetime import date
from typing import Literal

class LabResult(BaseModel):
    sample_id: str
    test_date: date
    result: str
    viral_load: int
    sample_status: Literal['keep', 'remove'] = 'keep'

    @field_validator('result')
    def check_result_code(cls, v):
        allowed = ['POS', 'NEG', 'N/A']
        if v not in allowed:
            raise ValueError(f"Invalid result code: '{v}'. Must be POS, NEG, or N/A")
        return v
    
    @field_validator('sample_status')
    def check_sample_status(cls, v):
        allowed = ['keep', 'remove']
        if v not in allowed:
            raise ValueError(f"Invalid sample_status: '{v}'. Must be 'keep' or 'remove'")
        return v