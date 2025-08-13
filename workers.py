# workers.py
"""
Workers Module: Contains the task execution logic.

Responsibilities:
- Process a block of tuples assigned by the Master.
- Query the fileManager for progress to avoid duplicate work.
- Use the api_client to perform queries.CD
- Send each result to the fileManager to be written to disk.
"""
import pandas as pd
from typing import List, Tuple, Dict
from threading import Lock

from api_client import get_coordinates, get_google_distance_time
from file_manager import read_worker_progress, write_worker_result

def _process_one_tuple(single_tuple: Tuple[int, int], coords_df: pd.DataFrame, api_key: str) -> Dict:
    """Private function to process a single tuple. This is the elemental job."""
    origin_zone, dest_zone = single_tuple
    result_dict = {'zat_ori': origin_zone, 'zat_des': dest_zone, 'distance_m': None, 'duration_s': None, 'error_api': None}
    
    origin_coords = get_coordinates(origin_zone, coords_df)
    dest_coords = get_coordinates(dest_zone, coords_df)

    if origin_coords and dest_coords:
        api_data = get_google_distance_time(origin_coords, dest_coords, api_key)
        result_dict.update(api_data)
    else:
        errors = []
        if not origin_coords: errors.append(f"Origin '{origin_zone}' no coords")
        if not dest_coords: errors.append(f"Dest '{dest_zone}' no coords")
        result_dict['error_api'] = ". ".join(errors)
        
    return result_dict

def execute_worker_job(worker_id: int, assigned_job: List[Tuple[int, int]], coords_df: pd.DataFrame, api_key: str, lock: Lock):
    """
    This is the main function executed by a worker thread.
    It receives a block of work and processes it.
    """
    # 1. Ask the FileManager about previous progress.
    already_done_tuples = read_worker_progress(worker_id)
    
    total_assigned = len(assigned_job)
    work_to_do = [t for t in assigned_job if t not in already_done_tuples]
    
    print(f"👷 WORKER {worker_id}: Task received. {total_assigned} total tuples. {len(work_to_do)} remaining.")

    if not work_to_do:
        print(f"✅ WORKER {worker_id}: No new work, task finished.")
        return f"Worker {worker_id} completed (no new tasks)."

    # 2. Iterate over the pending work and process it.
    for i, single_tuple in enumerate(work_to_do):
        result = _process_one_tuple(single_tuple, coords_df, api_key)
        
        # 3. Send the result to the FileManager to be written.
        write_worker_result(worker_id, result, lock)
        print(f"   WORKER {worker_id}: Processing {i+1}/{len(work_to_do)}...", end='\r')

    print(f"\n✅ WORKER {worker_id}: Job finished.")
    return f"Worker {worker_id} completed ({len(work_to_do)} new tasks)."