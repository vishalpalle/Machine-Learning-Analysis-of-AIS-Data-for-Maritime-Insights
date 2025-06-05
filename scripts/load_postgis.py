# File: ~/ais_vm/scripts/load_postgis.py

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine
from geoalchemy2 import Geometry


def main():
    # 1) Read raw CSV
    csv_path = os.path.join(os.path.dirname(__file__), "../data/ais_cleaned.csv")
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # 2) Lowercase all column names (so that "MMSI" → "mmsi", etc.)
    df.columns = df.columns.str.lower()

    # 3) If there are any remaining mismatches (e.g., 'basedatetime' 
    #    instead of 'timestamp', or 'lat'/ 'lon' instead of 'latitude'/'longitude'), rename them:
    df = df.rename(columns={
        "basedatetime": "timestamp",
        "lat": "latitude",
        "lon": "longitude",
        # You can add more if your CSV uses other variants:
        # "vesselname": "vesselname",  # (already lowercase)
        # "imo": "imo",
        # "callsign": "callsign",
        # "vesseltype": "vesseltype",
        # "status": "status",
        # "length": "length",
        # "width": "width",
        # "draft": "draft",
        # "cargo": "cargo",
        # "transceiverclass": "transceiverclass"
    })

    # 4) Confirm that all required columns (mmsi, timestamp, latitude, longitude, etc.) now exist:
    expected_cols = {
        "mmsi", "timestamp", "latitude", "longitude",
        "sog", "cog", "heading", "vesselname", "imo", 
        "callsign", "vesseltype", "status", "length", 
        "width", "draft", "cargo", "transceiverclass"
    }
    missing = expected_cols - set(df.columns)
    if missing:
        raise KeyError(f"The following required columns are missing in CSV: {missing}")

    # 5) Create a 'geom' column from lowercase lon/lat
    df["geom"] = df.apply(
        lambda row: Point(row["longitude"], row["latitude"]), axis=1
    )

    # 6) Convert to GeoDataFrame, telling GeoPandas that "geom" is the geometry column
    gdf = gpd.GeoDataFrame(
        df,
        geometry="geom",
        crs="EPSG:4326"  # WGS84
    )

    # 7) Create SQLAlchemy engine (update password as needed)
    engine = create_engine("postgresql://aisuser:changeme@localhost:5432/aisdb")

    # 8) Write to PostGIS (append existing rows):
    #    • name="ais_clean"     (your existing table)
    #    • schema="public"
    #    • dtype={"geom": Geometry("POINT", srid=4326)} ensures SRID=4326 on 'geom'
    gdf.to_postgis(
        name="ais_clean",
        con=engine,
        if_exists="append",
        index=False,
        schema="public",
        dtype={"geom": Geometry("POINT", srid=4326)}
    )

    print(f"Successfully loaded {len(gdf)} rows into public.ais_clean")


if __name__ == "__main__":
    main()
