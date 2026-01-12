#!/usr/bin/env python3
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import os
from search_engine import search_csv_files

app = FastAPI(title="The Big Hole - Search Engine")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup templates
templates = Jinja2Templates(directory="templates")


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
    
    # Map search types to column indices
    column_map = {
        "fbid": 0,
        "phone": 3,
        "name": 1  # Assuming name is in column 1
    }
    
    column_idx = column_map.get(search_type)
    
    if column_idx is None:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": "Invalid search type"
            }
        )
    
    # Execute search
    results = search_csv_files(search_term, column_idx)
    
    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "search_term": search_term,
            "search_type": search_type,
            "results": results,
            "total_results": len(results)
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4000, workers=1)
