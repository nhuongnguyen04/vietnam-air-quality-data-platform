import os
import pandas as pd
import numpy as np
import osmium
from scipy.spatial import cKDTree
import gc

# 1. Coordinate calculation helper
class RoadHandler(osmium.SimpleHandler):
    def __init__(self):
        super(RoadHandler, self).__init__()
        self.points = []
        self.types = []
        self.MAJOR_ROAD_TYPES = {
            'motorway', 'trunk', 'primary', 'secondary', 'tertiary', 
            'residential', 'living_street', 'unclassified'
        }

    def way(self, w):
        if 'highway' in w.tags:
            hw = w.tags.get('highway')
            # Expanded resolution: Include secondary and tertiary for better urban coverage
            if hw in self.MAJOR_ROAD_TYPES:
                # For ways, we need to get locations of nodes.
                # Since we don't have a full location cache (to save RAM), 
                # we rely on pyosmium's ability to provide node locations if requested.
                try:
                    for n in w.nodes:
                        if n.location.valid():
                            self.points.append([n.location.lat, n.location.lon])
                            self.types.append(hw)
                except osmium.InvalidLocationError:
                    pass

def preprocess_osm():
    osm_path = "data/osm/vietnam-latest.osm.pbf"
    wards_path = "dbt/dbt_tranform/seeds/vietnam_wards_2026.csv"
    output_path = "dbt/dbt_tranform/seeds/vietnam_wards_with_osm.csv"

    if not os.path.exists(osm_path):
        print(f"Error: {osm_path} not found.")
        return

    print("Loading wards...")
    wards_df = pd.read_csv(wards_path)
    # Drop wards without coordinates to avoid KDTree errors
    initial_count = len(wards_df)
    wards_df = wards_df.dropna(subset=['lat', 'lon'])
    if len(wards_df) < initial_count:
        print(f"Dropped {initial_count - len(wards_df)} wards with missing coordinates.")
    
    print("Initialize osmium stream parser (Low Resolution)...")
    handler = RoadHandler()
    # apply_file with locations=True enables node coordinates on ways
    handler.apply_file(osm_path, locations=True)
    
    print(f"Extracted {len(handler.points)} major road nodes.")

    if not handler.points:
        print("Error: No major roads found.")
        return

    print("Building spatial index (KDTree)...")
    tree = cKDTree(handler.points)

    print("Mapping wards to nearest roads...")
    results = []
    for idx, row in wards_df.iterrows():
        try:
            # Query nearest road node
            dist_deg, index = tree.query([row['lat'], row['lon']])
            
            # Convert degree distance to KM roughly (1 deg ~ 111 km)
            dist_km = dist_deg * 111.0
            
            res = row.to_dict()
            res['nearest_highway_type'] = handler.types[index]
            res['distance_to_road_km'] = round(float(dist_km), 4)
            res['snapped_lat'] = handler.points[index][0]
            res['snapped_lon'] = handler.points[index][1]
            results.append(res)
        except Exception as e:
            print(f"Skipping ward {row.get('code')} due to error: {e}")
    
    # Clean up to free RAM
    del handler
    del tree
    gc.collect()

    final_df = pd.DataFrame(results)
    final_df.to_csv(output_path, index=False)
    print(f"SUCCESS: Preprocessing complete. Saved to {output_path}")

if __name__ == "__main__":
    preprocess_osm()
