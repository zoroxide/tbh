#!/usr/bin/env python3
"""
High-performance MongoDB search engine
Optimized with indexes for sub-100ms query times
Target: < 100ms search time
"""

import os
import re
from typing import List, Dict, Any
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection
MONGODB_URL = os.getenv('MONGODB_URL')
MONGODB_NAME = os.getenv('MONGODB_NAME', 'fb-eg')

# Initialize MongoDB client (connection pooling)
_client = None
_db = None
_collection = None


def get_collection():
    """Get MongoDB collection with connection pooling"""
    global _client, _db, _collection
    
    if _collection is None:
        _client = MongoClient(
            MONGODB_URL,
            maxPoolSize=50,
            minPoolSize=10,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000
        )
        _db = _client[MONGODB_NAME]
        _collection = _db['users']
    
    return _collection


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


def format_user_result(doc: Dict) -> Dict[str, Any]:
    """Format MongoDB document to user result format"""
    return {
        'fbid': doc.get('fbid', 'N/A'),
        'name': doc.get('name', 'N/A'),
        'phone': doc.get('phone', 'N/A'),
        'email': doc.get('email', 'N/A'),
        'school': doc.get('school', 'N/A'),
        'location': doc.get('location', 'N/A'),
        'gender': doc.get('gender', 'N/A'),
        'profile_url': doc.get('profile_url', 'N/A'),
    }


def search_csv_files(search_term: str, search_type: str) -> List[Dict[str, Any]]:
    """
    Search MongoDB database for a specific term.
    Uses indexes for O(1) lookup time.
    
    Args:
        search_term: The term to search for
        search_type: Type of search (phone, fbid, name)
        
    Returns:
        List of all matching records
    """
    try:
        collection = get_collection()
        results = []
        
        if search_type == 'phone':
            # Search phone with normalization
            search_variants = normalize_phone(search_term)
            query = {'phone': {'$in': search_variants}}
            
            # Use index for fast lookup
            docs = collection.find(query).limit(100)
            results = [format_user_result(doc) for doc in docs]
        
        elif search_type == 'fbid':
            # Search FBID and email with normalization
            search_variants = normalize_fbid(search_term)
            query = {
                '$or': [
                    {'fbid': {'$in': search_variants}},
                    {'email': {'$in': search_variants}}
                ]
            }
            
            # Use index for fast lookup
            docs = collection.find(query).limit(100)
            results = [format_user_result(doc) for doc in docs]
        
        elif search_type == 'name':
            # Search name with regex for partial matching
            search_term_clean = re.escape(search_term.strip())
            query = {
                '$or': [
                    {'name': {'$regex': search_term_clean, '$options': 'i'}},
                    {'first_name': {'$regex': search_term_clean, '$options': 'i'}},
                    {'last_name': {'$regex': search_term_clean, '$options': 'i'}}
                ]
            }
            
            # Regex search with limit
            docs = collection.find(query).limit(100)
            results = [format_user_result(doc) for doc in docs]
        
        return results
    
    except Exception as e:
        print(f"MongoDB search error: {e}")
        return []


# CLI interface for testing
if __name__ == "__main__":
    import sys
    import time
    
    if len(sys.argv) != 3:
        print("Usage: python3 search_engine.py <search_type> <search_term>")
        print("Search types: phone, fbid, name")
        print("Examples:")
        print("  python3 search_engine.py phone 01146537372")
        print("  python3 search_engine.py fbid loay.mohamed.12764874")
        print("  python3 search_engine.py name Loay")
        sys.exit(1)
    
    search_type = sys.argv[1]
    search_term = sys.argv[2]
    
    if search_type not in ['phone', 'fbid', 'name']:
        print("Error: search_type must be 'phone', 'fbid', or 'name'")
        sys.exit(1)
    
    print(f"Searching for '{search_term}' by {search_type}...")
    print("Using MongoDB with indexes...")
    
    start_time = time.time()
    results = search_csv_files(search_term, search_type)
    elapsed = time.time() - start_time
    
    print(f"\n{'='*80}")
    print(f"Search completed in {elapsed*1000:.2f}ms ({elapsed:.4f} seconds)")
    print(f"Found {len(results)} match(es)")
    print(f"{'='*80}\n")
    
    for i, user in enumerate(results, 1):
        print(f"User #{i}:")
        print(f"  Name: {user['name']}")
        print(f"  Phone: {user['phone']}")
        print(f"  Email: {user['email']}")
        print(f"  FBID: {user['fbid']}")
        print(f"  School: {user['school']}")
        print(f"  Location: {user['location']}")
        print()
