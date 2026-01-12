#!/usr/bin/env python3
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import os
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from search_engine import search_csv_files

app = FastAPI(title="The Big Hole - Search Engine")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup templates
templates = Jinja2Templates(directory="templates")

# Thread pool for running blocking operations
executor = ThreadPoolExecutor(max_workers=1)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main search page"""
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


@app.post("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    search_type: str = Form(...),
    search_term: str = Form(...)
):
    """Handle search requests"""
    
    if not search_term.strip():
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": "Search term cannot be empty"
            }
        )
    
    # Validate search type
    if search_type not in ['fbid', 'phone', 'name']:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": "Invalid search type"
            }
        )
    
    # Execute search asynchronously and track time
    start_time = time.time()
    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(executor, search_csv_files, search_term, search_type)
    end_time = time.time()
    
    # Calculate search time in milliseconds
    search_time_ms = int((end_time - start_time) * 1000)
    
    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "search_term": search_term,
            "search_type": search_type,
            "results": results,
            "total_results": len(results),
            "search_time_ms": search_time_ms
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4000, workers=1, timeout_keep_alive=300)
