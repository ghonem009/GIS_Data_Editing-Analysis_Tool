from fastapi import APIRouter, HTTPException, UploadFile, File
from app.core.gis_manager import GISManager
from app.schemas.feature_schemas import FeatureCreate, CRSModel
from app.config import DATA_DIR
from pydantic import BaseModel
import os 

router = APIRouter(prefix="/feature", tags=["Features"])
gis = GISManager()


@router.post("/add")
def add_feature(data : FeatureCreate):
    feature_id = gis.add_feature(data.geometry, data.properties)
    return {
        "status": "success",
        "feature_id": feature_id
    }


@router.post("/reproject")
def reproject_data(crs : CRSModel):
    gis.reproject(crs.target_crs)
    return {
        "status": "success",
        "crs": crs.target_crs
    }


@router.get("/show")
def show_dataset():
    return gis.gdf.to_json()


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
