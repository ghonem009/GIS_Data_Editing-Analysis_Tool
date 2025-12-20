from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from app.core.gis_manager import GISManager
from app.schemas.feature_schemas import FeatureCreate, CRSModel, BufferRequest, GeometryRequest, UnionRequest, SimplifyRequest, DissolveRequest
from app.config import DATA_DIR
import os 
import json

router = APIRouter(prefix="/feature", tags=["Features"])
gis = GISManager()


# ==>> featurs endpoints
@router.post("/add")
def add_feature(data : FeatureCreate):
    feature_id = gis.add_feature(data.geometry, data.properties)
    return {
        "status": "success",
        "feature_id": feature_id
    }



@router.delete("/{feature_id}")
def delete_feature(feature_id: int):
    deleted = gis.delete_feature(feature_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Feature not found")
    return {"status": "deleted", "feature_id": feature_id}



@router.post("/reproject")
def reproject_data(crs : CRSModel):
    gis.reproject(crs.target_crs)
    return {
        "status": "success",
        "crs": crs.target_crs
    }


@router.get("/show")
def show_dataset():
    geojson = json.loads(gis.gdf.to_json())
    return JSONResponse(content=geojson)



@router.post("/upload")
def upload_dataset(file: UploadFile = File(...)):
    filename = file.filename
    file_path = os.path.join(DATA_DIR, filename)

    content = file.file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    gis.load_dataset(file_path)

    return {
        "status": "success",
        "filename": filename
    }


# ==>> geometry oprations endpoints
@router.post("/analysis/buffer")
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
    


@router.post("/analysis/clip")
def clip_operation(data: GeometryRequest):
    clipped = gis.clip(data.geometry)
    clipped_geojson = json.loads(clipped.to_json())
    return {
        "status": "success",
        "operation": "clip",
        "features": clipped_geojson,
        "clipped": clipped
    }


@router.post("/analysis/intersect")
def intersect_operation(data: GeometryRequest):
    intersected = gis.intersect(data.geometry)
    intersected_geojson = json.loads(intersected.to_json())
    return {
        "status": "success",
        "operation": "intersect",
        "features": intersected_geojson,
        "count": len(intersected)
    }


@router.post("/analysis/union")
def union_operation(data: UnionRequest):
    from shapely.geometry import mapping
    
    union_geom = gis.union(feature_ids=data.feature_ids)
    union_geojson = mapping(union_geom)
    
    return {
        "status": "success",
        "operation": "union",
        "feature_ids": data.feature_ids,
        "result_geometry": union_geojson,
        "geometry_type": union_geom.geom_type
    }


@router.post("/analysis/simplify")
def simplify_operation(data: SimplifyRequest):
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


@router.post("/analysis/dissolve")
def dissolve_operation(data: DissolveRequest):
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



