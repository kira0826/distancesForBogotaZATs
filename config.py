# config.py
"""
Global configuration module.

Contains all adjustable variables and constants for the project,
such as API keys, file paths, and execution parameters.
"""
import os


API_KEY = os.getenv("GOOGLE_API_KEY")

NUM_WORKERS = 5


INPUT_DIR = "files"
BLOCKS_DIR = "blocks"  

FILE_TUPLE_NAME = "zat_ori_dest.txt"
TUPLES_FILEPATH = os.path.join(INPUT_DIR, FILE_TUPLE_NAME)
COORDS_FILEPATH = os.path.join(INPUT_DIR, "lat_lon_for_zat.xlsx")
OUTPUT_FILEPATH = os.path.join(INPUT_DIR, "result.csv")