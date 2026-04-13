"""Aplicación FastAPI — lectura de narrativas."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from vixion.api.routes import router

app = FastAPI(
    title="VIXION Read API",
    version="0.1.0",
    description="Lectura mínima de narrative_current e historial para inspección manual.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:3000",
        "http://localhost:3000",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
