from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class AnalysisResultCreate(BaseModel):
    operation_type: str = Field(...)
    source_feature_ids: Optional[List[int]] = Field(default=None)
    parameters: Dict[str, Any]


class AnalysisResultResponse(BaseModel):
    result_id: int
    operation_type: str
    source_feature_ids: Optional[list[int]]
    parameters: Dict[str, Any]
    feature_count: int

    class Config:
        from_attributes = True

