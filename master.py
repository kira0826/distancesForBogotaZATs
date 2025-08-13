# master.py
"""
Master Module: Entry point and orchestrator of the process.

Responsibilities:
- Initiate the process.
- Tell the FileManager to load the data.
- Split the total dataset into work blocks.
- Assign each block to a Worker (thread).
- Wait for all Workers to finish.
- Tell the FileManager to consolidate the results.
"""
import os
import time
import math
import concurrent.futures
from threading import Lock

from config import API_KEY, NUM_WORKERS, TUPLES_FILEPATH, COORDS_FILEPATH, INPUT_DIR, BLOCKS_DIR
from file_manager import load_initial_data, consolidate_final_work
from workers import execute_worker_job

def main():
    """Main function that orchestrates the entire process."""
    print("👑 MASTER: Initiating process...")
    if not API_KEY or "YOUR_API_KEY" in API_KEY:
        print("👑 MASTER: 🛑 ERROR: Please set your `API_KEY` in config.py.")
        return

    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(BLOCKS_DIR, exist_ok=True)

    # 1. Tell the FileManager to prepare the data.
    tuples_df, coords_df = load_initial_data(TUPLES_FILEPATH, COORDS_FILEPATH)
    if tuples_df is None: return

    # 2. Divide the total workload.
    full_tuple_list = list(tuples_df.itertuples(index=False, name=None))
    if not full_tuple_list:
        print("👑 MASTER: ⚠️ No tuples to process.")
        return
    
    block_size = math.ceil(len(full_tuple_list) / NUM_WORKERS)
    work_blocks = [
        full_tuple_list[i:i + block_size]
        for i in range(0, len(full_tuple_list), block_size)
    ]
    
    print(f"👑 MASTER: Work divided into {len(work_blocks)} blocks.")
    
    # Create a Lock to be shared among all threads for safe file writing.
    file_lock = Lock()

    # 3. Assign work to the Workers.
    print("👑 MASTER: Assigning work to workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit the worker function with its arguments
        futures = [
            executor.submit(execute_worker_job, i, block, coords_df, API_KEY, file_lock)
            for i, block in enumerate(work_blocks)
        ]
        # Wait for and collect confirmations
        for future in concurrent.futures.as_completed(futures):
            try:
                print(f"👑 MASTER: Report received -> {future.result()}")
            except Exception as e:
                print(f"👑 MASTER: 🔥 ERROR reported by a worker: {e}")

    # 4. Tell the FileManager to consolidate everything.
    consolidate_final_work()

if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\n👑 MASTER: Process finished in {(end_time - start_time) / 60:.2f} minutes.")