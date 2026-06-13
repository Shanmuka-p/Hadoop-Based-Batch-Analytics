#!/bin/sh
if [ -s /data/cdr_data.csv ]; then
    echo "Data exists. Skipping."
    exit 0
fi
python3 /data/generate_records.py