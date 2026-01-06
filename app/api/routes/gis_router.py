import asyncio
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from fastapi.responses import JSONResponse
from app.core.gis_manager import GISManager
from app.schemas.feature_schemas import (FeatureCreate,FeatureUpdate,BufferRequest,GeometryRequest,UnionRequest,SimplifyRequest,DissolveRequest)
from app.config import DATA_DIR
import os
import json
from shapely.geometry import mapping
import geopandas as gpd
from shapely.validation import make_valid
import math
import pandas as pd

feature_router = APIRouter(prefix="/feature", tags=["Feature Editing"])
analysis_router = APIRouter(prefix="/analysis", tags=["Spatial Analysis"])

gis = GISManager()


# ==>> features endpoints

@feature_router.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    try:
        allowed = [".geojson", ".shp"]
        _, ext = os.path.splitext(file.filename.lower())
        if ext not in allowed:
            raise HTTPException(status_code=400, detail="Only GeoJSON or Shapefile formats are allowed.")

        path = os.path.join(DATA_DIR, file.filename)
        with open(path, "wb") as f:
            f.write(await file.read())

        gdf = gpd.read_file(path)
        if gdf.empty:
            raise HTTPException(status_code=400, detail="The uploaded file is empty or invalid.")

        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            gdf = gdf.set_crs(epsg=4326, allow_override=True)

        gdf["properties"] = gdf.drop(columns=["geometry"]).apply(lambda x: x.to_dict(), axis=1)

        await gis.load_from_db()
        max_id = gis.gdf["feature_id"].max() + 1 if not gis.gdf.empty else 1

        gdf.insert(0, "feature_id", range(int(max_id), int(max_id) + len(gdf)))
        gdf = gdf[["feature_id", "properties", "geometry"]]

        def clean_json(obj):
            for k, v in obj.items():
                if isinstance(v, float) and math.isnan(v):
                    obj[k] = None
            return json.dumps(obj)

        gdf["properties"] = gdf["properties"].apply(clean_json)

        gdf.to_postgis(
            name=gis.features_table,
            con=gis.sync_engine,
            if_exists="append",
            index=False,
            dtype={"geometry": Geometry("GEOMETRY", srid=4326), "properties": JSONB()}
        )

        if os.path.exists(path):
            os.remove(path)

        return {"status": "success", "count": len(gdf), "message": "Dataset uploaded successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


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
        data(FeatureUpdate): new geometry and/or properties
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
    delete a feature by id
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
    """
    return:
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
async def intersect_operation(data: GeometryRequest):
    try:
        intersected = await gis.intersect(data.geometry)
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
    try:
        table_name, clipped = await gis.clip(data.geometry)
        return {
            "status": "success",
            "operation": "clip",
            "features": json.loads(clipped.to_json()),
            "count": len(clipped)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Clip operation failed: {str(e)}")


@analysis_router.post("/nearest")
async def nearest_operation(data: GeometryRequest):
    try:
        result = await gis.nearest_neighbor(data.geometry)
        if result is None:
            raise HTTPException(status_code=404, detail="No features found for nearest neighbor")
        return {
            "status": "success",
            "operation": "nearest",
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Nearest operation failed: {str(e)}")


@analysis_router.post("/spatial-join")
async def spatial_join_endpoint(other_file: UploadFile = File(...)):
    try:
        path = os.path.join(DATA_DIR, other_file.filename)
        content = other_file.file.read()
        with open(path, "wb") as f:
            f.write(content)

        other_gdf = gpd.read_file(path)
        joined = await gis.spatial_join(other_gdf)

        return {
            "status": "success",
            "operation": "spatial_join",
            "joined_count": len(joined),
            "features": json.loads(joined.to_json())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Spatial join failed: {str(e)}")


@analysis_router.post("/union")
async def union_operation(data: UnionRequest):
    try:
        union_geom = await gis.union(feature_ids=data.feature_ids)
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
    try:
        result_id, simplified = await gis.simplification(tolerance=data.tolerance)
        return {
            "status": "success",
            "operation": "simplify",
            "tolerance": data.tolerance,
            "result_id": result_id,
            "count": len(simplified)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Simplify operation failed: {str(e)}")


@analysis_router.post("/dissolve")
async def dissolve_operation(data: DissolveRequest):
    try:
        result_id, dissolved = await gis.dissolve(by=data.by)
        return {
            "status": "success",
            "operation": "dissolve",
            "attribute": data.by,
            "result_id": result_id,
            "count": len(dissolved)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dissolve operation failed: {str(e)}")
