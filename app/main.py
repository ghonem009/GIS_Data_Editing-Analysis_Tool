from fastapi import FastAPI
from app.api.routes.gis_router import feature_router, analysis_router
from app.core.gis_manager import GISManager

app = FastAPI(title="GIS Backend")

app.include_router(feature_router)
app.include_router(analysis_router)

gis = GISManager()

@app.on_event("startup")
async def setup_db():
    await gis.tables_exist()

@app.get("/")
def home():
    return {"status": "API running"}
