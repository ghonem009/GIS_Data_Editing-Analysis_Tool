from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from app.core.gis_manager import GISManager
from app.schemas.feature_schemas import (FeatureCreate,FeatureUpdate,BufferRequest,GeometryRequest,UnionRequest,SimplifyRequest,DissolveRequest)
from app.config import DATA_DIR
import os
import json
from shapely.geometry import mapping
import geopandas as gpd
from fastapi import Query


feature_router = APIRouter(prefix="/feature", tags=["Feature Editing"])
analysis_router = APIRouter(prefix="/analysis", tags=["Spatial Analysis"])

gis = GISManager()


# ==>> features endpoints
@feature_router.post("/add")
async def add_feature(data: FeatureCreate):
    """
    add a new feature
    args:
        data (FeatureCreate): geometry, properties, fix_topology 
    returns:
        dict: status and created feature_id
    """
    try:
        feature_id = await gis.add_feature(
            data.geometry,
            data.properties,
            fix_topology=data.fix_topology
        )
        return {"status": "success", "feature_id": feature_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Add feature failed: {str(e)}")


@feature_router.put("/{feature_id}/update")
async def update_feature(feature_id: int, data: FeatureUpdate):
    """
    update an existing feature
    args:
        feature_id: int 
        data(FeatureCreate): new geometry and/or properties
    returns:
        dict : status and updated feature_id
    """
    try:
        updated_id = await gis.update_feature(
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
async def delete_feature(feature_id: int):
    """
    Delete a feature by id
    args:
        features_id: int
    return:
        dict: status and deleted feature_id 
    """
    try:
        deleted = await gis.delete_feature(feature_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Feature not found")
        return {"status": "success", "feature_id": feature_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@feature_router.get("/show")
def show_features():
    """return:
        JSONResponse: all features as geojson
    """
    try:
        geojson = json.loads(gis.gdf.to_json())
        return JSONResponse(content=geojson)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load features: {str(e)}")

# ==>> spatial analysis endpoints
@analysis_router.post("/buffer")
async def buffer_operation(data: BufferRequest):
    """
    create buffer zones around features
    args:
        data (BufferRequest): distance and optional feature_id
    returns:
        dict: status, operation name, and result layer id
    """
    try:
        result_id, _ = await gis.buffer(
            distance=data.distance,
            feature_id=data.feature_id
        )
        return {
            "status": "success",
            "operation": "buffer",
            "distance": data.distance,
            "feature_id": data.feature_id,
            "result_id": result_id,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Buffer operation failed: {str(e)}")


@analysis_router.post("/intersect")
def intersect_operation(data: GeometryRequest):
    """
    intersect existing features with a geometry
    args:
        data (GeometryRequest): input geometry
    returns:
        dict: status, count, resulting features
    """
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
async def clip_operation(data: GeometryRequest):
    """
    clip features by an input geometry
    args:
        data (GeometryRequest): clip boundary geometry
    returns:
        dict: Status, count, and resulting clipped features
    """
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
    """
    find nearest feature to a given geometry
    args:
        data (GeometryRequest): input geometry
    returns:
        dict: Nearest feature details
    """
    try:
        result = gis.nearest_neighbor(data.geometry)
        if result is None:
            raise HTTPException(status_code=404, detail="No features found for nearest neighbor")
        return {"status": "success", "operation": "nearest", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Nearest operation failed: {str(e)}")


@analysis_router.post("/spatial-join")
def spatial_join_endpoint(other_file: UploadFile = File(...)):
    """
    perform a spatial join with an uploaded dataset

    args:
        other_file (uploadFile): spatial file (geojson or shapefile, ...)
    returns:
        dict: joined features and count
    """
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
    """
    Union multiple features into one geometry

    args:
        data (unionRequest): List of feature ids
    returns:
        dict: union geometry and type
    """
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
async def simplify_operation(data: SimplifyRequest):
    """
    simplify geometries.

    args:
        data (SimplifyRequest): tolerance and simplify options
    returns:
        dict: Simplification status and tolerance used
    """
    try:
        await gis.simplification(
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
    """
    dissolve features by an attribute
    args:
        data (DissolveRequest): attribute name

    returns:
        dict: counts before and after dissolve
    """  
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


@analysis_router.get("/results")
def show_analysis_results():
    """
    show results of previous analysis operations

    Returns:
        dict: count and result features
    """
    try:
        results = gis.get_analysis_results()
        if results.empty:
            return {"status": "success", "count": 0, "message": "No results yet"}

        return {
            "status": "success",
            "count": len(results),
            "results": json.loads(results.to_json())
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
