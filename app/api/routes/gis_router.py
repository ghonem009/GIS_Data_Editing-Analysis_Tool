from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from app.core.gis_manager import GISManager
from app.schemas.feature_schemas import (FeatureCreate,FeatureUpdate,BufferRequest,GeometryRequest,UnionRequest,SimplifyRequest,DissolveRequest)
from app.config import DATA_DIR
import os
import json
from shapely.geometry import mapping
import geopandas as gpd

feature_router = APIRouter(prefix="/feature", tags=["Feature Editing"])
analysis_router = APIRouter(prefix="/analysis", tags=["Spatial Analysis"])

gis = GISManager()


# ==>> featurs endpoints
@feature_router.post("/add")
def add_feature(data: FeatureCreate):
    """Add new feature to the database."""
    try:
        feature_id = gis.add_feature(data.geometry, data.properties, fix_topology=data.fix_topology)
        return {"status": "success", "feature_id": feature_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Add feature failed: {str(e)}")


@feature_router.put("/{feature_id}/update")
def update_feature(feature_id: int, data: FeatureUpdate):
    try:
        updated_id = gis.update_feature(
            feature_id,
            new_geom=data.geometry,
            new_properties=data.properties,
            fix_topology=data.fix_topology
        )
        return {"status": "success", "feature_id": updated_id}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update feature failed: {str(e)}")



@feature_router.delete("/{feature_id}/delete")
def delete_feature(feature_id: int):
    """Delete a feature by ID."""
    try:
        deleted = gis.delete_feature(feature_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Feature not found")
        return {"status": "success", "feature_id": feature_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")



@feature_router.get("/show")
def show_features():
    """Return all features as GeoJSON."""
    try:
        geojson = json.loads(gis.gdf.to_json())
        return JSONResponse(content=geojson)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load features: {str(e)}")

# ==>> spatial analysis endpoints
@analysis_router.post("/buffer")
def buffer_operation(data: BufferRequest):
    try:
        gis.buffer(distance=data.distance, feature_id=data.feature_id)
        return {
            "status": "success",
            "operation": "buffer",
            "distance": data.distance,
            "feature_id": data.feature_id
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Buffer operation failed: {str(e)}")


@analysis_router.post("/intersect")
def intersect_operation(data: GeometryRequest):
    try:
        intersected = gis.intersect(data.geometry)
        return {
            "status": "success",
            "operation": "intersect",
            "count": len(intersected),
            "features": json.loads(intersected.to_json())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Intersect operation failed: {str(e)}")


@analysis_router.post("/clip")
def clip_operation(data: GeometryRequest):
    try:
        clipped = gis.clip(data.geometry)
        return {
            "status": "success",
            "operation": "clip",
            "features": json.loads(clipped.to_json()),
            "count": len(clipped)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Clip operation failed: {str(e)}")


@analysis_router.post("/nearest")
def nearest_operation(data: GeometryRequest):
    try:
        result = gis.nearest_neighbor(data.geometry)
        if result is None:
            raise HTTPException(status_code=404, detail="No features found for nearest neighbor")
        return {"status": "success", "operation": "nearest", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Nearest operation failed: {str(e)}")


@analysis_router.post("/spatial-join")
def spatial_join_endpoint(other_file: UploadFile = File(...)):
    try:
        path = os.path.join(DATA_DIR, other_file.filename)
        content = other_file.file.read()
        with open(path, "wb") as f:
            f.write(content)
        other_gdf = gpd.read_file(path)
        joined = gis.spatial_join(other_gdf)
        return {
            "status": "success",
            "operation": "spatial_join",
            "joined_count": len(joined),
            "features": json.loads(joined.to_json())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Spatial join failed: {str(e)}")


@analysis_router.post("/union")
def union_operation(data: UnionRequest):
    try:
        union_geom = gis.union(feature_ids=data.feature_ids)
        if union_geom is None:
            raise HTTPException(status_code=400, detail="No features to union")
        return {
            "status": "success",
            "operation": "union",
            "feature_ids": data.feature_ids,
            "geometry_type": union_geom.geom_type,
            "result_geometry": mapping(union_geom)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Union operation failed: {str(e)}")


@analysis_router.post("/simplify")
def simplify_operation(data: SimplifyRequest):
    try:
        gis.simplification(
            tolerance=data.tolerance,
            simplify_coverage=data.simplify_coverage,
            simplify_boundary=data.simplify_boundary
        )
        return {
            "status": "success",
            "operation": "simplify",
            "tolerance": data.tolerance
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Simplify operation failed: {str(e)}")


@analysis_router.post("/dissolve")
def dissolve_operation(data: DissolveRequest):
    try:
        original_count = len(gis.gdf)
        gis.dissolve(by=data.by)
        dissolved_count = len(gis.gdf)
        return {
            "status": "success",
            "operation": "dissolve",
            "attribute": data.by,
            "original_features": original_count,
            "dissolved_features": dissolved_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dissolve operation failed: {str(e)}")
