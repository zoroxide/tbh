#!/bin/bash

echo "================================================================"
echo " THE BIG HOLE - Search Engine Startup"
echo "================================================================"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "[*] Creating virtual environment..."
    python3 -m venv venv
    echo "[+] Virtual environment created!"
    echo ""
fi

# Activate virtual environment
echo "[*] Activating virtual environment..."
source venv/bin/activate

# Install/Update requirements
echo "[*] Installing dependencies..."
pip install -r requirements.txt
echo ""

# Start the server
echo "================================================================"
echo " Starting FastAPI Server..."
echo " Access at: http://localhost:8000"
echo "================================================================"
echo ""
python3 main.py
