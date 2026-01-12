#!/usr/bin/env python3
from fastapi import FastAPI, Request, Form, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
import os
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
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


def check_auth(request: Request) -> bool:
    """Check if user is authenticated"""
    return request.session.get("authenticated", False)


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
async def index(request: Request):
    """Render the main search page"""
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=303)
    
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
    uvicorn.run(app, host="0.0.0.0", port=80, workers=1, timeout_keep_alive=300)
