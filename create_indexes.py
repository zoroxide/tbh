#!/usr/bin/env python3
"""
Create MongoDB indexes for ultra-fast searches
Run this after importing data to ensure optimal performance
"""

from pymongo import MongoClient, ASCENDING, TEXT
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

MONGODB_URL = os.getenv('MONGODB_URL')
MONGODB_NAME = os.getenv('MONGODB_NAME', 'fb-eg')

def create_indexes():
    """Create all necessary indexes for fast searching"""
    
    print("="*70)
    print("MONGODB INDEX CREATION")
    print("="*70)
    
    try:
        # Connect to MongoDB
        print(f"\n🔗 Connecting to MongoDB...")
        client = MongoClient(MONGODB_URL, serverSelectionTimeoutMS=5000)
        db = client[MONGODB_NAME]
        collection = db['users']
        
        # Test connection
        client.admin.command('ping')
        print(f"   ✅ Connected to database: {MONGODB_NAME}")
        
        # Get current document count
        count = collection.count_documents({})
        print(f"   📊 Total documents: {count:,}")
        
        # Drop existing indexes (except _id)
        print(f"\n🗑️  Dropping old indexes...")
        collection.drop_indexes()
        print(f"   ✅ Old indexes removed")
        
        # Create new indexes
        print(f"\n📊 Creating optimized indexes...")
        
        # 1. FBID - Unique index for exact match (most important)
        print(f"   Creating index: fbid (unique)")
        collection.create_index('fbid', unique=True, sparse=True)
        
        # 2. Phone - Index for exact match
        print(f"   Creating index: phone")
        collection.create_index('phone', sparse=True)
        
        # 3. Email - Index for exact match
        print(f"   Creating index: email")
        collection.create_index('email', sparse=True)
        
        # 4. Name - Index for partial match
        print(f"   Creating index: name")
        collection.create_index('name', sparse=True)
        
        # 5. First name - Index for partial match
        print(f"   Creating index: first_name")
        collection.create_index('first_name', sparse=True)
        
        # 6. Last name - Index for partial match
        print(f"   Creating index: last_name")
        collection.create_index('last_name', sparse=True)
        
        # 7. Compound index for common queries
        print(f"   Creating compound index: phone + email")
        collection.create_index([('phone', ASCENDING), ('email', ASCENDING)], sparse=True)
        
        print(f"\n   ✅ All indexes created successfully!")
        
        # List all indexes
        print(f"\n📋 Current indexes:")
        for index in collection.list_indexes():
            print(f"   - {index['name']}: {index.get('key', {})}")
        
        # Test query speed
        print(f"\n⚡ Testing query performance...")
        
        # Get a sample phone number
        sample = collection.find_one({'phone': {'$exists': True, '$ne': None}})
        if sample and sample.get('phone'):
            import time
            test_phone = sample['phone']
            
            start = time.time()
            result = collection.find_one({'phone': test_phone})
            elapsed = (time.time() - start) * 1000
            
            print(f"   Query by phone: {elapsed:.2f}ms")
            
            if elapsed < 100:
                print(f"   ✅ Performance: EXCELLENT (< 100ms)")
            elif elapsed < 500:
                print(f"   ✅ Performance: GOOD (< 500ms)")
            else:
                print(f"   ⚠️  Performance: NEEDS OPTIMIZATION (> 500ms)")
        
        print(f"\n{'='*70}")
        print("INDEX SETUP COMPLETED")
        print("Your database is now optimized for lightning-fast searches!")
        print("="*70)
        
        client.close()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False
    
    return True


if __name__ == "__main__":
    create_indexes()
