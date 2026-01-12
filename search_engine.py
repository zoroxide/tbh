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

# Use all available cores (16 cores = 16 threads for I/O bound operations)
# We can even use more threads since CSV reading is I/O bound
MAX_WORKERS = 24  # Optimal for 16-core system with I/O operations

# Thread-safe results storage
results_lock = threading.Lock()


def search_file_chunk(file_path: str, search_term: str, column_idx: int, 
                      start_line: int, end_line: int, chunk_id: int) -> List[Dict[str, Any]]:
    """
    Search a specific chunk of a CSV file for matching records.
    
    Args:
        file_path: Path to CSV file
        search_term: Term to search for
        column_idx: Column index to search in
        start_line: Starting line number (inclusive)
        end_line: Ending line number (exclusive)
        chunk_id: Identifier for this chunk
        
    Returns:
        List of matching records with metadata
    """
    matches = []
    
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            
            # Skip header
            header = next(reader, None)
            
            # Skip to start_line
            for _ in range(start_line - 1):
                try:
                    next(reader)
                except StopIteration:
                    return matches
            
            # Search within chunk
            current_line = start_line
            for row in reader:
                if current_line >= end_line:
                    break
                    
                try:
                    # Check if the search term matches
                    if len(row) > column_idx and row[column_idx] == search_term:
                        matches.append({
                            "file": os.path.basename(file_path),
                            "line": current_line + 1,  # +1 to account for header
                            "row": ",".join(row)
                        })
                except (IndexError, Exception):
                    pass
                
                current_line += 1
                
    except (FileNotFoundError, IOError):
        pass
    
    return matches


def count_file_lines(file_path: str) -> int:
    """
    Quickly count lines in a file.
    
    Args:
        file_path: Path to file
        
    Returns:
        Number of lines in file
    """
    try:
        with open(file_path, "rb") as f:
            return sum(1 for _ in f)
    except (FileNotFoundError, IOError):
        return 0


def search_file_parallel(file_path: str, search_term: str, column_idx: int,
                         chunks_per_file: int = 4) -> List[Dict[str, Any]]:
    """
    Search a single file using multiple threads by splitting into chunks.
    
    Args:
        file_path: Path to CSV file
        search_term: Term to search for
        column_idx: Column index to search in
        chunks_per_file: Number of chunks to split each file into
        
    Returns:
        List of all matching records from this file
    """
    if not os.path.exists(file_path):
        return []
    
    # Count total lines
    total_lines = count_file_lines(file_path)
    if total_lines <= 1:  # Only header or empty
        return []
    
    # Calculate chunk size (excluding header)
    data_lines = total_lines - 1
    chunk_size = max(1, data_lines // chunks_per_file)
    
    all_matches = []
    
    # Create chunks
    with ThreadPoolExecutor(max_workers=chunks_per_file) as executor:
        futures = []
        
        for i in range(chunks_per_file):
            start_line = i * chunk_size + 1  # +1 for header
            end_line = start_line + chunk_size if i < chunks_per_file - 1 else total_lines
            
            future = executor.submit(
                search_file_chunk,
                file_path,
                search_term,
                column_idx,
                start_line,
                end_line,
                i
            )
            futures.append(future)
        
        # Collect results
        for future in as_completed(futures):
            try:
                matches = future.result()
                all_matches.extend(matches)
            except Exception as e:
                print(f"Error processing chunk: {e}")
    
    return all_matches


def search_csv_files(search_term: str, column_idx: int) -> List[Dict[str, Any]]:
    """
    Search all CSV files in parallel for a specific term.
    
    This function uses a two-level parallelization strategy:
    1. Process multiple files simultaneously
    2. Split each large file into chunks for parallel processing
    
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
    
    # Process all files in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(search_file_parallel, file_path, search_term, column_idx, 4): file_path
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
