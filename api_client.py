# api_client.py
"""
Module for interacting with the Google Maps API.

Responsibilities:
- Get coordinates for a specific Zone ID (ZAT).
- Make the call to the Distance Matrix API and parse the response.
"""
import requests
import pandas as pd
from typing import List, Dict, Optional

def get_coordinates(zone_id: int, coords_df: pd.DataFrame) -> Optional[List[float]]:
    """Fetches the [longitude, latitude] coordinates for a specific zone."""
    coords = coords_df[coords_df['ZAT'].astype(int) == int(zone_id)]
    if not coords.empty:
        return [coords.iloc[0]['lon'], coords.iloc[0]['lat']]
    return None

def get_google_distance_time(origin_coords: List[float], dest_coords: List[float], api_key: str) -> Dict:
    """Queries the Google Distance Matrix API for a pair of coordinates."""
    base_url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    # API requires "latitude,longitude" format
    origin_str = f"{origin_coords[1]},{origin_coords[0]}"
    dest_str = f"{dest_coords[1]},{dest_coords[0]}"
    params = {'origins': origin_str, 'destinations': dest_str, 'key': api_key}
    
    try:
        response = requests.get(base_url, params=params, timeout=15)
        response.raise_for_status() # Raise an exception for 4xx/5xx errors
        data = response.json()
        if data['status'] == 'OK' and data['rows'][0]['elements'][0]['status'] == 'OK':
            element = data['rows'][0]['elements'][0]
            return {'distance_m': element['distance']['value'], 'duration_s': element['duration']['value']}
        else:
            status = data.get('error_message', data.get('status', 'Unknown Error'))
            return {'error_api': f"Google API Error: {status}"}
    except requests.exceptions.RequestException as e:
        return {'error_api': f"Connection Error: {e}"}