# fileManager.py
"""
FileManager Module: Handles all interaction with the file system.

Responsibilities:
- Load initial data (tuples and coordinates).
- Read a worker's progress to allow for job resumption.
- Safely write a worker's results to its block file.
- Consolidate all block files into a final result.
"""
import pandas as pd
import os
import ast
import csv
from typing import Tuple, Optional, Set, Dict
from threading import Lock

from config import BLOCKS_DIR, OUTPUT_FILEPATH

def load_initial_data(tuples_file: str, coords_file: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Loads the initial tuples and coordinates from source files."""
    print("FILEMANAGER: Loading initial data...")
    try:
        with open(tuples_file, 'r', encoding='utf-8') as f:
            tuples_list = [ast.literal_eval(line.strip()) for line in f]
        tuples_df = pd.DataFrame(tuples_list, columns=['zat_ori', 'zat_des'])
        coords_df = pd.read_excel(coords_file)
        print(f"FILEMANAGER: ✅ Load complete. {len(tuples_df)} tuples and {len(coords_df)} coordinates found.")
        return tuples_df, coords_df
    except FileNotFoundError as e:
        print(f"FILEMANAGER: 🛑 Critical Error. File not found: {e.filename}.")
        return None, None

def read_worker_progress(worker_id: int) -> Set[Tuple[int, int]]:
    """Reads a worker's progress file and returns a set of completed tuples."""
    worker_file = os.path.join(BLOCKS_DIR, f"worker_{worker_id}_progress.csv")
    processed_tuples: Set[Tuple[int, int]] = set()
    if os.path.exists(worker_file):
        try:
            with open(worker_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=';')
                next(reader)  # Skip header
                for row in reader:
                    processed_tuples.add((int(row[0]), int(row[1])))
        except (IOError, IndexError, StopIteration):
            # If file is corrupt or empty, it will be treated as new.
            pass
    return processed_tuples

def write_worker_result(worker_id: int, result_dict: Dict, lock: Lock):
    """Safely writes a result row to a worker's progress file."""
    worker_file = os.path.join(BLOCKS_DIR, f"worker_{worker_id}_progress.csv")
    
    # The lock ensures that only one thread can write to a file at a time,
    # preventing file corruption.
    with lock:
        is_new_file = not os.path.exists(worker_file) or os.path.getsize(worker_file) == 0
        
        with open(worker_file, 'a', newline='', encoding='utf-8') as f:
            headers = ['zat_ori', 'zat_des', 'distance_m', 'duration_s', 'error_api']
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=';')
            if is_new_file:
                writer.writeheader()
            writer.writerow(result_dict)

def consolidate_final_work():
    """Merges all worker progress files into a single output file."""
    print("\nFILEMANAGER: Consolidating work from all workers...")
    all_dfs = []
    for filename in os.listdir(BLOCKS_DIR):
        if filename.startswith("worker_") and filename.endswith(".csv"):
            try:
                full_path = os.path.join(BLOCKS_DIR, filename)
                block_df = pd.read_csv(full_path, sep=';')
                all_dfs.append(block_df)
            except pd.errors.EmptyDataError:
                pass # Ignore empty files

    if not all_dfs:
        print("FILEMANAGER: 🛑 No data found to consolidate.")
        return
        
    consolidated_df = pd.concat(all_dfs, ignore_index=True)
    consolidated_df.to_csv(OUTPUT_FILEPATH, index=False, sep=';', encoding='utf-8')
    print(f"FILEMANAGER: ✅ Work consolidated. Final output: '{OUTPUT_FILEPATH}'")