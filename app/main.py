from fastapi import FastAPI
from app.api.routes.gis_router import feature_router, analysis_router  

app = FastAPI(title="GIS Backend")

app.include_router(feature_router)
app.include_router(analysis_router)

@app.get("/")
def home():
    return {"status": "API running"}
