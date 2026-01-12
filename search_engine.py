#!/usr/bin/env python3
"""
High-performance multithreaded CSV search engine
Optimized for 12GB+ datasets on 16-core, 64GB RAM systems
Target: < 10 seconds search time
"""

import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import threading

# -------- CONFIG --------
CSV_DIR = "csv"
FILES = ["eg-1.csv", "eg-2.csv", "eg-3.csv", "eg-4.csv"]

# Increase CSV field limit
csv.field_size_limit(sys.maxsize)

# Use multiple workers for parallel file processing
MAX_WORKERS = 4  # One thread per file for simplicity and reliability

# Thread-safe results storage
results_lock = threading.Lock()


def search_single_file(file_path: str, search_term: str, column_idx: int) -> List[Dict[str, Any]]:
    """
    Search a single CSV file for matching records.
    Simplified and optimized for large files (11M+ rows).
    
    Args:
        file_path: Path to CSV file
        search_term: Term to search for
        column_idx: Column index to search in
        
    Returns:
        List of matching records with metadata
    """
    matches = []
    
    if not os.path.exists(file_path):
        return matches
    
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            
            # Skip header
            header = next(reader, None)
            
            # Search all rows
            for row_num, row in enumerate(reader, start=2):  # start=2 to account for header
                try:
                    # Check if the search term matches
                    if len(row) > column_idx and row[column_idx] == search_term:
                        matches.append({
                            "file": os.path.basename(file_path),
                            "line": row_num,
                            "row": ",".join(row)
                        })
                except (IndexError, Exception):
                    continue
                    
    except (FileNotFoundError, IOError, Exception) as e:
        print(f"Error reading file {file_path}: {e}")
    
    return matches


def search_csv_files(search_term: str, column_idx: int) -> List[Dict[str, Any]]:
    """
    Search all CSV files in parallel for a specific term.
    
    Simple and reliable approach: one thread per file.
    With 4 files and 4 threads, this is efficient and straightforward.
    
    Args:
        search_term: The term to search for
        column_idx: Column index to search in (0=FBID, 1=Name, 3=Phone)
        
    Returns:
        List of all matching records across all files
    """
    all_results = []
    
    # Get all file paths
    file_paths = [os.path.join(CSV_DIR, fname) for fname in FILES]
    
    # Add any additional CSV files in the directory
    try:
        all_csv_files = [
            os.path.join(CSV_DIR, f) 
            for f in os.listdir(CSV_DIR) 
            if f.endswith('.csv')
        ]
        # Merge and deduplicate
        file_paths = list(set(file_paths + all_csv_files))
    except (FileNotFoundError, PermissionError):
        pass
    
    # Filter to existing files
    file_paths = [fp for fp in file_paths if os.path.exists(fp)]
    
    if not file_paths:
        return []
    
    # Process all files in parallel (one thread per file)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(search_single_file, file_path, search_term, column_idx): file_path
            for file_path in file_paths
        }
        
        for future in as_completed(futures):
            file_path = futures[future]
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
    
    return all_results


def search_csv_files_simple(search_term: str, column_idx: int) -> List[Dict[str, Any]]:
    """
    Simple single-threaded search (fallback or for small datasets).
    
    Args:
        search_term: The term to search for
        column_idx: Column index to search in
        
    Returns:
        List of all matching records
    """
    results = []
    
    for fname in FILES:
        path = os.path.join(CSV_DIR, fname)
        
        if not os.path.exists(path):
            continue
        
        try:
            with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)  # skip header
                
                for row_num, row in enumerate(reader, start=2):
                    try:
                        if len(row) > column_idx and row[column_idx] == search_term:
                            results.append({
                                "file": fname,
                                "line": row_num,
                                "row": ",".join(row)
                            })
                    except IndexError:
                        continue
        except (FileNotFoundError, IOError):
            continue
    
    return results


# CLI interface for testing
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 search_engine.py <column_idx> <search_term>")
        print("Column indices: 0=FBID, 1=Name, 3=Phone")
        sys.exit(1)
    
    column_idx = int(sys.argv[1])
    search_term = sys.argv[2]
    
    print(f"Searching for '{search_term}' in column {column_idx}...")
    print("Using multithreaded search engine...")
    
    import time
    start_time = time.time()
    
    results = search_csv_files(search_term, column_idx)
    
    elapsed = time.time() - start_time
    
    print(f"\n{'='*80}")
    print(f"Search completed in {elapsed:.2f} seconds")
    print(f"Found {len(results)} match(es)")
    print(f"{'='*80}\n")
    
    for i, result in enumerate(results, 1):
        print(f"Result #{i}:")
        print(f"  File: {result['file']}")
        print(f"  Line: {result['line']}")
        print(f"  Data: {result['row']}")
        print()
