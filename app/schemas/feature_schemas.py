from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class FeatureCreate(BaseModel):
    geometry: Dict[str, Any]
    properties: Dict[str, Any] = Field(default_factory=dict)
    fix_topology: bool = False 


class FeatureUpdate(BaseModel):
    geometry: Optional[Dict[str, Any]] = None
    properties: Optional[Dict[str, Any]] = None
    fix_topology: bool = False


class BufferRequest(BaseModel):
    distance: float
    feature_id: Optional[int] = None 


class GeometryRequest(BaseModel):
    geometry: Dict[str, Any]


class SimplifyRequest(BaseModel):
    tolerance: float
    simplify_coverage: bool = True
    simplify_boundary: bool = True


class DissolveRequest(BaseModel):
    by: str


class UnionRequest(BaseModel):
    feature_ids: Optional[List[int]] = None  