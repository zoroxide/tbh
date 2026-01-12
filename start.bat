@echo off
echo ================================================================
echo  THE BIG HOLE - Search Engine Startup
echo ================================================================
echo.

REM Check if virtual environment exists
if not exist "venv" (
    echo [*] Creating virtual environment...
    python -m venv venv
    echo [+] Virtual environment created!
    echo.
)

REM Activate virtual environment
echo [*] Activating virtual environment...
call venv\Scripts\activate.bat

REM Install/Update requirements
echo [*] Installing dependencies...
pip install -r requirements.txt
echo.

REM Start the server
echo ================================================================
echo  Starting FastAPI Server...
echo  Access at: http://localhost:8000
echo ================================================================
echo.
python main.py

pause
