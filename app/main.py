from fastapi import FastAPI
from app.api.routes import gis_router

app = FastAPI(title="GIS Backend")

app.include_router(gis_router.router)

@app.get("/")
def home():
    return {"status": "API running"}
