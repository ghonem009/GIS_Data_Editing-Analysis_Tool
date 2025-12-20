from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class FeatureCreate(BaseModel):
    geometry: Dict[str, Any]
    properties: Dict[str, Any] = Field(default_factory=dict)

class CRSModel(BaseModel):
    target_crs : str = "EPSG:4326"


class BufferRequest(BaseModel):
    distance: float
    feature_id: Optional[int]


class GeometryRequest(BaseModel):
    geometry: Dict[str, Any]


class SimplifyRequest(BaseModel):
    tolerance: float
    simplify_coverage: bool 
    simplify_boundary: bool

class DissolveRequest(BaseModel):
    by: str


class UnionRequest(BaseModel):
    feature_ids: Optional[List[int]] 