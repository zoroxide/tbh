#!/usr/bin/env python3
import csv
import sys
import os

# -------- CONFIG --------
CSV_DIR = "csv"
FILES = ["eg-1.csv", "eg-2.csv", "eg-3.csv", "eg-4.csv"]

FBID_COL = 0
PHONE_COL = 3

# Increase CSV field limit (legacy safety)
csv.field_size_limit(sys.maxsize)

# -------- ARG --------
if len(sys.argv) != 2:
    print("Usage: python3 lookup.py <phone_or_fbid>")
    sys.exit(1)

search_term = sys.argv[1]

found = False

# -------- SEARCH --------
for fname in FILES:
    path = os.path.join(CSV_DIR, fname)

    if not os.path.exists(path):
        continue

    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)

        header = next(reader, None)  # skip header

        for row_num, row in enumerate(reader, start=2):
            try:
                if row[FBID_COL] == search_term or row[PHONE_COL] == search_term:
                    print(f"\n✅ MATCH FOUND")
                    print(f"📄 File : {fname}")
                    print(f"📍 Line : {row_num}")
                    print(f"🧾 Row  :")
                    print(",".join(row))
                    found = True
            except IndexError:
                continue

if not found:
    print("❌ No match found")