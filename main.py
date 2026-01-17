#!/usr/bin/env python3
from fastapi import FastAPI, Request, Form, Response, Query
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import os
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from search_engine import search_csv_files

app = FastAPI(title="The Big Hole - Search Engine")

# Add session middleware
app.add_middleware(SessionMiddleware, secret_key="lF_66K9_8bn_secret_key_for_sessions")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup templates
templates = Jinja2Templates(directory="templates")

# Thread pool for running blocking operations
executor = ThreadPoolExecutor(max_workers=1)

# Authentication token
VALID_TOKEN = "lF=!66K9\\8bn"

# Pagination settings
RESULTS_PER_PAGE = 10


def check_auth(request: Request) -> bool:
    """Check if user is authenticated"""
    return request.session.get("authenticated", False)


def paginate_results(results, page, per_page):
    """Paginate results list"""
    total = len(results)
    total_pages = (total + per_page - 1) // per_page  # Ceiling division
    start = (page - 1) * per_page
    end = start + per_page
    
    return {
        'items': results[start:end],
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'has_prev': page > 1,
        'has_next': page < total_pages
    }


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Render the login page"""
    return templates.TemplateResponse(
        "login.html",
        {"request": request}
    )


@app.post("/login", response_class=HTMLResponse)
async def login(request: Request, token: str = Form(...)):
    """Handle login"""
    if token == VALID_TOKEN:
        request.session["authenticated"] = True
        return RedirectResponse(url="/", status_code=303)
    else:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Invalid token. Access denied."
            }
        )


@app.get("/logout")
async def logout(request: Request):
    """Handle logout"""
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    search_type: Optional[str] = Query(None),
    search_term: Optional[str] = Query(None),
    page: int = Query(1, ge=1)
):
    """Render the main search page with optional search results"""
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=303)
    
    # If no search parameters, just show the form
    if not search_term or not search_type:
        return templates.TemplateResponse(
            "index.html",
            {"request": request}
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
    
    # Paginate results
    pagination = paginate_results(results, page, RESULTS_PER_PAGE)
    
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "search_term": search_term,
            "search_type": search_type,
            "results": pagination['items'],
            "total_results": pagination['total'],
            "search_time_ms": search_time_ms,
            "page": pagination['page'],
            "total_pages": pagination['total_pages'],
            "has_prev": pagination['has_prev'],
            "has_next": pagination['has_next']
        }
    )


@app.post("/", response_class=HTMLResponse)
async def search_post(
    request: Request,
    search_type: str = Form(...),
    search_term: str = Form(...)
):
    """Handle search form submission and redirect to GET with query params"""
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=303)
    
    if not search_term.strip():
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": "Search term cannot be empty"
            }
        )
    
    # Redirect to GET request with query parameters
    return RedirectResponse(
        url=f"/?search_type={search_type}&search_term={search_term.strip()}&page=1",
        status_code=303
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80, workers=1, timeout_keep_alive=300)

