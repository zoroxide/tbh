# THE BIG HOLE - High-Performance Search Engine

A server-side rendered web application with a hacker/terminal theme for searching through massive CSV datasets.

## Features

- 🔥 **Multithreaded Search Engine**: Optimized for 12GB+ CSV files
- ⚡ **Fast Performance**: Target < 10 seconds on 16-core, 64GB RAM systems
- 🎨 **Hacker Terminal Theme**: ASCII art, green-on-black terminal aesthetics
- 🔍 **Multiple Search Types**: Search by Name, Facebook ID, or Phone Number
- 📊 **Real-time Results**: Server-side rendering with Jinja2 templates

## Architecture

### Performance Optimization
- **Two-level Parallelization**:
  - Level 1: Multiple CSV files processed simultaneously
  - Level 2: Each large file split into chunks for parallel processing
- **24 Worker Threads**: Optimized for 16-core systems (I/O-bound operations benefit from more threads than cores)
- **Efficient File Reading**: Memory-efficient streaming with CSV reader
- **Smart Chunking**: Dynamic chunk sizing based on file size

### Tech Stack
- **Backend**: FastAPI (async Python web framework)
- **Templating**: Jinja2 (server-side rendering)
- **Styling**: Custom CSS with VT323 font (retro terminal look)
- **Search Engine**: Custom multithreaded CSV scanner

## Project Structure

```
the big hole/
├── main.py                 # FastAPI application entry point
├── search_engine.py        # Multithreaded search engine
├── search.py              # Original single-threaded version (legacy)
├── requirements.txt       # Python dependencies
├── templates/
│   ├── base.html         # Base template with terminal UI
│   ├── index.html        # Search form page
│   └── results.html      # Search results page
├── static/
│   └── style.css         # Hacker terminal theme styles
└── csv/                   # CSV data directory
    ├── eg-1.csv
    ├── eg-2.csv
    ├── eg-3.csv
    └── eg-4.csv
```

## Installation

### Prerequisites
- Python 3.8+
- 16-core CPU with 64GB RAM (recommended for optimal performance)
- CSV files in the `csv/` directory

### Setup

1. **Install Dependencies**
```bash
pip install -r requirements.txt
```

2. **Prepare CSV Files**
   - Place your CSV files in the `csv/` directory
   - Expected CSV structure:
     - Column 0: Facebook ID (FBID)
     - Column 1: Name
     - Column 3: Phone Number

3. **Run the Application**
```bash
python main.py
```

Or using uvicorn directly:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

4. **Access the Application**
   - Open browser to: http://localhost:8000
   - The terminal-themed interface will load

## Usage

1. **Select Search Type**
   - Choose from dropdown: Name, FBID, or Phone Number

2. **Enter Search Term**
   - Type exact search term (case-sensitive)

3. **Execute Search**
   - Click "EXECUTE SEARCH" button
   - Results will appear in < 10 seconds

4. **View Results**
   - See all matching records with file name, line number, and full row data
   - Results displayed in hacker terminal style

## Performance Tuning

### For Different Hardware Configurations

Edit `search_engine.py` to adjust thread count:

```python
# For 8-core systems
MAX_WORKERS = 12

# For 16-core systems (default)
MAX_WORKERS = 24

# For 32-core systems
MAX_WORKERS = 48
```

### For Smaller Datasets

If your CSV files are < 1GB total, you can use the simpler search:

In `main.py`, replace:
```python
results = search_csv_files(search_term, column_idx)
```

With:
```python
results = search_csv_files_simple(search_term, column_idx)
```

## Configuration

### CSV Configuration

Edit `search_engine.py` to modify CSV settings:

```python
CSV_DIR = "csv"  # Directory containing CSV files
FILES = ["eg-1.csv", "eg-2.csv", "eg-3.csv", "eg-4.csv"]  # File list

# Column indices (zero-based)
FBID_COL = 0
NAME_COL = 1
PHONE_COL = 3
```

### Search Type Mapping

In `main.py`, modify column mapping:

```python
column_map = {
    "fbid": 0,    # Facebook ID column
    "phone": 3,   # Phone number column
    "name": 1     # Name column
}
```

## API Endpoints

### GET `/`
Renders the main search page with form

### POST `/search`
Handles search requests

**Form Parameters:**
- `search_type`: "name" | "fbid" | "phone"
- `search_term`: String to search for

**Returns:**
- HTML page with search results

## Troubleshooting

### Performance Issues

1. **Search taking > 10 seconds?**
   - Check CPU usage during search
   - Increase `MAX_WORKERS` if CPU is underutilized
   - Ensure CSV files are on fast storage (SSD/NVMe)

2. **Memory Issues**
   - Reduce `chunks_per_file` in `search_file_parallel()`
   - Monitor RAM usage during search

3. **File Not Found Errors**
   - Verify CSV files are in `csv/` directory
   - Check file permissions

### UI Issues

1. **Fonts not loading?**
   - Check internet connection (Google Fonts required)
   - Or download VT323 font locally

2. **Styling broken?**
   - Clear browser cache
   - Verify `/static` directory exists

## Security Notes

⚠️ **This is a search tool for authorized use only**

- No authentication implemented (add if needed)
- All searches should be logged
- Ensure CSV data is properly secured
- Use HTTPS in production

## License

Proprietary - Internal Use Only

## Support

For issues or questions, contact the system administrator.

---

**Status**: ✅ OPERATIONAL | **Version**: 2.0 | **Updated**: 2026-01-12
