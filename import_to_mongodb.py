#!/usr/bin/env python3
"""
CSV to MongoDB Import Script
Imports large CSV files into MongoDB Atlas with progress tracking
"""

import csv
import os
import sys
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# MongoDB settings
MONGODB_URL = os.getenv('MONGODB_URL')
MONGODB_NAME = os.getenv('MONGODB_NAME')

# CSV settings
CSV_DIR = "csv"
FILES = ["eg-1.csv", "eg-2.csv", "eg-3.csv", "eg-4.csv"]

# Batch size for bulk inserts (optimal for performance)
BATCH_SIZE = 10000

# Increase CSV field limit
csv.field_size_limit(sys.maxsize)


def get_line_count(file_path):
    """Quickly count lines in a file for progress bar"""
    print(f"Counting lines in {os.path.basename(file_path)}...")
    with open(file_path, 'rb') as f:
        count = sum(1 for _ in f)
    return count - 1  # Subtract header


def parse_row_to_document(row):
    """
    Parse CSV row into MongoDB document
    
    CSV Structure (based on sample):
    0: fbid, 1-2: unknown, 3: phone, 4-5: unknown, 
    6: first_name, 7: last_name, 8: gender, 9: profile_url,
    10-15: unknown, 16: location, 17: school, 18: unknown,
    19: email, 20+: additional fields
    """
    try:
        doc = {
            'fbid': row[0] if len(row) > 0 else None,
            'phone': row[3] if len(row) > 3 else None,
            'first_name': row[6] if len(row) > 6 else None,
            'last_name': row[7] if len(row) > 7 else None,
            'name': f"{row[6]} {row[7]}".strip() if len(row) > 7 else None,
            'gender': row[8] if len(row) > 8 else None,
            'profile_url': row[9] if len(row) > 9 else None,
            'location': row[16] if len(row) > 16 else None,
            'school': row[17] if len(row) > 17 else None,
            'email': row[19] if len(row) > 19 else None,
        }
        
        # Remove None values to save space
        doc = {k: v for k, v in doc.items() if v}
        
        return doc
    except Exception as e:
        print(f"Error parsing row: {e}")
        return None


def import_csv_to_mongodb(file_path, collection):
    """Import a single CSV file into MongoDB collection"""
    filename = os.path.basename(file_path)
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return 0
    
    # Count lines for progress bar
    total_lines = get_line_count(file_path)
    
    print(f"\n📄 Importing {filename}")
    print(f"   Total rows: {total_lines:,}")
    
    imported = 0
    batch = []
    
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            
            # Skip header
            next(reader, None)
            
            # Process rows with progress bar
            with tqdm(total=total_lines, desc=f"  {filename}", unit=" rows") as pbar:
                for row in reader:
                    doc = parse_row_to_document(row)
                    
                    if doc and doc.get('fbid'):  # Only import if has FBID
                        batch.append(doc)
                    
                    # Bulk insert when batch is full
                    if len(batch) >= BATCH_SIZE:
                        try:
                            collection.insert_many(batch, ordered=False)
                            imported += len(batch)
                            batch = []
                        except Exception as e:
                            # Handle duplicate key errors gracefully
                            if 'duplicate key' not in str(e).lower():
                                print(f"\n⚠ Batch insert error: {e}")
                            imported += len(batch)
                            batch = []
                    
                    pbar.update(1)
                
                # Insert remaining batch
                if batch:
                    try:
                        collection.insert_many(batch, ordered=False)
                        imported += len(batch)
                    except Exception as e:
                        if 'duplicate key' not in str(e).lower():
                            print(f"\n⚠ Final batch error: {e}")
                        imported += len(batch)
    
    except Exception as e:
        print(f"\n❌ Error reading file {filename}: {e}")
    
    print(f"   ✅ Imported: {imported:,} records")
    return imported


def main():
    """Main import function"""
    print("="*70)
    print("CSV TO MONGODB IMPORT SCRIPT")
    print("="*70)
    
    # Validate environment variables
    if not MONGODB_URL or '<db_password>' in MONGODB_URL:
        print("❌ Error: Please set MONGODB_URL in .env file with valid password")
        sys.exit(1)
    
    if not MONGODB_NAME:
        print("❌ Error: Please set MONGODB_NAME in .env file")
        sys.exit(1)
    
    print(f"\n🔗 Connecting to MongoDB Atlas...")
    print(f"   Database: {MONGODB_NAME}")
    
    try:
        # Connect to MongoDB
        client = MongoClient(MONGODB_URL)
        db = client[MONGODB_NAME]
        collection = db['users']
        
        # Test connection
        client.admin.command('ping')
        print(f"   ✅ Connected successfully!")
        
        # Create indexes for fast searching
        print(f"\n📊 Creating indexes...")
        collection.create_index('fbid', unique=True)
        collection.create_index('phone')
        collection.create_index('email')
        collection.create_index('name')
        print(f"   ✅ Indexes created!")
        
        # Import all CSV files
        total_imported = 0
        
        for filename in FILES:
            file_path = os.path.join(CSV_DIR, filename)
            imported = import_csv_to_mongodb(file_path, collection)
            total_imported += imported
        
        # Final stats
        print("\n" + "="*70)
        print("IMPORT COMPLETED")
        print("="*70)
        print(f"Total records imported: {total_imported:,}")
        print(f"Total records in database: {collection.count_documents({}):,}")
        print("="*70)
        
        client.close()
        
    except Exception as e:
        print(f"\n❌ MongoDB connection error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("\n⚠  WARNING: This will import large CSV files into MongoDB")
    print("   This process may take 30-60 minutes for ~44 million rows")
    response = input("\n   Continue? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        main()
    else:
        print("\n❌ Import cancelled")
