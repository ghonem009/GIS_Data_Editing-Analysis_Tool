from pydantic import BaseModel
from typing import Dict, Any
 
class FeatureCreate(BaseModel):
    geometry: Dict[str, Any]
    properties: Dict[str, Any] = {}


class CRSModel(BaseModel):
    target_crs : str = "EPSG:4326"