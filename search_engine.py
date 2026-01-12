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

# CSV Column mapping (based on the sample data)
COL_FBID = 0
COL_NAME_PARTS = [6, 7]  # First name, Last name
COL_PHONE = 3
COL_EMAIL = 19
COL_SCHOOL = 17
COL_LOCATION = 16
COL_GENDER = 8
COL_PROFILE_URL = 9


def normalize_phone(phone: str) -> List[str]:
    """
    Normalize phone number to handle variations.
    Returns list of possible formats to search.
    """
    phone = phone.strip()
    variants = [phone]
    
    # If starts with 0, add +20 prefix
    if phone.startswith('0'):
        variants.append('+20' + phone[1:])
    # If starts with +20, also search without it
    elif phone.startswith('+20'):
        variants.append('0' + phone[3:])
    
    return variants


def normalize_fbid(fbid: str) -> List[str]:
    """
    Normalize FBID to handle variations.
    Returns list of possible formats to search.
    """
    fbid = fbid.strip()
    variants = [fbid]
    
    # If has @facebook.com, also search without it
    if '@facebook.com' in fbid:
        variants.append(fbid.replace('@facebook.com', ''))
    else:
        # If doesn't have @facebook.com, add it
        variants.append(fbid + '@facebook.com')
    
    return variants


def parse_row_to_user(row: List[str]) -> Dict[str, Any]:
    """
    Parse CSV row into structured user data.
    """
    try:
        first_name = row[6] if len(row) > 6 else ''
        last_name = row[7] if len(row) > 7 else ''
        full_name = f"{first_name} {last_name}".strip()
        
        return {
            'fbid': row[0] if len(row) > 0 else '',
            'name': full_name or 'N/A',
            'phone': row[3] if len(row) > 3 else 'N/A',
            'email': row[19] if len(row) > 19 else 'N/A',
            'school': row[17] if len(row) > 17 else 'N/A',
            'location': row[16] if len(row) > 16 else 'N/A',
            'gender': row[8] if len(row) > 8 else 'N/A',
            'profile_url': row[9] if len(row) > 9 else 'N/A',
        }
    except Exception:
        return {
            'fbid': 'N/A',
            'name': 'N/A',
            'phone': 'N/A',
            'email': 'N/A',
            'school': 'N/A',
            'location': 'N/A',
            'gender': 'N/A',
            'profile_url': 'N/A',
        }


def search_single_file(file_path: str, search_term: str, search_type: str) -> List[Dict[str, Any]]:
    """
    Search a single CSV file for matching records.
    Simplified and optimized for large files (11M+ rows).
    
    Args:
        file_path: Path to CSV file
        search_term: Term to search for
        search_type: Type of search (phone, fbid, name)
        
    Returns:
        List of matching records with metadata
    """
    matches = []
    
    if not os.path.exists(file_path):
        return matches
    
    # Normalize search term based on type
    if search_type == 'phone':
        search_variants = normalize_phone(search_term)
    elif search_type == 'fbid':
        search_variants = normalize_fbid(search_term)
    else:  # name
        search_variants = [search_term.lower()]
    
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            
            # Skip header
            header = next(reader, None)
            
            # Search all rows
            for row_num, row in enumerate(reader, start=2):
                try:
                    match_found = False
                    
                    if search_type == 'phone':
                        # Check phone column
                        if len(row) > COL_PHONE:
                            phone = row[COL_PHONE]
                            if phone in search_variants:
                                match_found = True
                    
                    elif search_type == 'fbid':
                        # Check both FBID column and email column
                        if len(row) > COL_FBID:
                            fbid = row[COL_FBID]
                            if fbid in search_variants:
                                match_found = True
                        if not match_found and len(row) > COL_EMAIL:
                            email = row[COL_EMAIL]
                            if email in search_variants:
                                match_found = True
                    
                    elif search_type == 'name':
                        # Check name columns (first + last name)
                        if len(row) > 7:
                            first_name = row[6].lower() if len(row) > 6 else ''
                            last_name = row[7].lower() if len(row) > 7 else ''
                            full_name = f"{first_name} {last_name}".strip()
                            
                            search_lower = search_term.lower()
                            if (search_lower in full_name or 
                                search_lower in first_name or 
                                search_lower in last_name):
                                match_found = True
                    
                    if match_found:
                        user_data = parse_row_to_user(row)
                        matches.append(user_data)
                        
                except (IndexError, Exception):
                    continue
                    
    except (FileNotFoundError, IOError, Exception) as e:
        print(f"Error reading file {file_path}: {e}")
    
    return matches


def search_csv_files(search_term: str, search_type: str) -> List[Dict[str, Any]]:
    """
    Search all CSV files in parallel for a specific term.
    
    Simple and reliable approach: one thread per file.
    With 4 files and 4 threads, this is efficient and straightforward.
    
    Args:
        search_term: The term to search for
        search_type: Type of search (phone, fbid, name)
        
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
            executor.submit(search_single_file, file_path, search_term, search_type): file_path
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
