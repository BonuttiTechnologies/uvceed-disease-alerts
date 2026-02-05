from fastapi import FastAPI
from .routes.health import router as health_router
from .routes.signals import router as signals_router

app = FastAPI(title="UVCeed API", version="0.1.0")

app.include_router(health_router)
app.include_router(signals_router)
