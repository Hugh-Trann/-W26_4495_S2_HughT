from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.load_raw import create_batch_and_load_raw
from src.transform_clean import run_raw_to_clean_for_batch
from src.build_analytics import rebuild_analytics

APP_DIR = Path(__file__).parent
UPLOAD_DIR = APP_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

app = FastAPI(title="IBP Upload UI")

# Static + templates
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

# In-memory "job status" for demo (replace with DB/Redis for production)
JOBS: dict[str, dict] = {}


@app.get("/", response_class=HTMLResponse)
def upload_page(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})


def _allowed(filename: str) -> bool:
    ext = filename.lower().rsplit(".", 1)[-1]
    return ext in {"csv", "xlsx"}


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)) -> JSONResponse:
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file selected.")
    if not _allowed(file.filename):
        raise HTTPException(status_code=400, detail="Only .csv or .xlsx files are allowed.")

    # Save to disk
    file_id = str(uuid.uuid4())
    safe_name = f"{file_id}__{Path(file.filename).name}"
    dest = UPLOAD_DIR / safe_name

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="File is empty.")
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Max file size is 50 MB.")

    dest.write_bytes(content)

    return JSONResponse(
        {
            "file_id": file_id,
            "original_name": file.filename,
            "stored_name": safe_name,
            "size_bytes": len(content),
        }
    )


@app.get("/api/preview")
def api_preview(file_id: str, rows: int = 15) -> JSONResponse:
    # Find stored file by file_id
    matches = list(UPLOAD_DIR.glob(f"{file_id}__*"))
    if not matches:
        raise HTTPException(status_code=404, detail="File not found.")
    path = matches[0]

    ext = path.suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(path)
        elif ext == ".xlsx":
            df = pd.read_excel(path)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")

    df = df.head(max(1, min(rows, 100)))
    # Convert NaN to None for JSON
    data = df.where(pd.notnull(df), None).to_dict(orient="records")
    return JSONResponse({"columns": list(df.columns), "rows": data, "row_count": len(df)})


@app.post("/api/run")
def api_run(file_id: str, dataset_type: str, dataset_year: int):
    matches = list(UPLOAD_DIR.glob(f"{file_id}__*"))
    if not matches:
        raise HTTPException(status_code=404, detail="File not found.")
    uploaded_path = str(matches[0])

    raw_result = create_batch_and_load_raw(
        file_path=uploaded_path,
        dataset_type=dataset_type,
        dataset_year=dataset_year
    )

    clean_result = run_raw_to_clean_for_batch(raw_result["batch_id"])

    analytics_result = rebuild_analytics()

    return JSONResponse({
    "status": "completed",
    "message": "Pipeline completed. RAW, CLEAN, and ANALYTICS updated in SQL Server.",
    "raw_result": raw_result,
    "clean_result": clean_result,
    "analytics_result": analytics_result
})



@app.get("/api/status")
def api_status(job_id: str) -> JSONResponse:
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return JSONResponse(job)