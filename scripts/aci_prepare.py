import sys
import os
import re
import argparse
import numpy as np
from pathlib import Path

import pandas as pd
import geopandas as gpd
import rasterio
from rasterstats import zonal_stats

# project convention for relative paths
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths

def choose_aci_for_year(aci_dir: Path, year: int) -> Path:
    candidates = sorted(aci_dir.glob(f"aci_{year}_mb_v*.tif"))
    if not candidates:
        raise FileNotFoundError(f"No ACI raster found in {aci_dir} matching aci_{year}_mb_v*.tif")
    def v_key(p: Path):
        m = re.search(r"_v(\d+)\.tif$", p.name)
        return int(m.group(1)) if m else -1
    return max(candidates, key=v_key)

def load_data(year: int):
    aci_lut_path = paths.reference() / "aci_crop_classifications_iac_classifications_des_cultures.csv"
    muni_path = paths.reference() / "municipalities.geojson"
    muni_rm_lut_path = paths.reference() / "muni_rm_lut.csv"
    aci_dir = paths.raw(year)

    aci_path = choose_aci_for_year(aci_dir, year)
    print(f"Using raster: {aci_path.name}")

    aci_lut = pd.read_csv(aci_lut_path)
    muni_gdf = gpd.read_file(muni_path)
    muni_rm_lut = pd.read_csv(muni_rm_lut_path)

    return aci_lut, muni_gdf, muni_rm_lut, aci_path

def summarize_aci_by_rm(aci_path, muni_gdf, aci_lut, year):
    import numpy as np

    # open raster and get metadata
    with rasterio.open(aci_path) as src:
        raster_crs = src.crs
        pixel_area_m2 = abs(src.transform.a * src.transform.e)

    # ensure CRS match
    if muni_gdf.crs != raster_crs:
        muni_gdf = muni_gdf.to_crs(raster_crs)

    # compute zonal stats
    stats = zonal_stats(
        muni_gdf,
        aci_path,
        categorical=True,
        all_touched=False,
        nodata=0
    )

    # flatten zonal stats results
    records = []
    for i, row in enumerate(muni_gdf.itertuples()):
        muni_no = row.MUNI_NO
        muni_name = row.MUNI_NAME
        zone_stats = stats[i]
        for code, count in zone_stats.items():
            records.append({
                "MUNI_NO": muni_no,
                "MUNI_NAME": muni_name,
                "Code": int(code),
                "pixel_count": int(count)
            })

    df = pd.DataFrame.from_records(records)

    # compute area conversions
    df["hectares"] = df["pixel_count"] * pixel_area_m2 / 10000
    df["acres"] = df["pixel_count"] * pixel_area_m2 / 4046.85642

    # map code to crop label
    aci_lut["Code"] = aci_lut["Code"].astype(int)
    df = df.merge(aci_lut[["Code", "Label"]], on="Code", how="left")

    # keep only agricultural labels
    label_set = [
    "Agriculture (undifferentiated)", "Pasture/forages", "Too wet to be seeded", "Fallow",
    "Cereals", "Barley", "Other grains", "Millet", "Oats", "Rye", "Spelt", "Triticale",
    "Wheat", "Winter wheat", "Spring wheat", "Oilseeds", "Borage", "Camelina",
    "Canola/rapeseed", "Flaxseed", "Mustard", "Safflower", "Sunflower", "Soybeans",
    "Pulses", "Peas", "Chickpeas", "Beans", "Fababeans", "Lentils", "Vegetables",
    "Tomatoes", "Potatoes", "Sugarbeets", "Other vegetables", "Fruits", "Sorghum",
    "Quinoa", "Corn", "Tobacco", "Ginseng", "Other pulses", "Berries", "Blueberry",
    "Cranberry", "Other berries", "Orchards", "Other fruits", "Vineyards", "Hops",
    "Sod", "Herbs", "Buckwheat", "Canaryseed", "Hemp", "Switchgrass", "Vetch",
    "Other crops"
]

    df = df[df["Label"].isin(label_set)]

    # save output
    output_path = paths.interim(year) / f"aci_summary_{year}.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved {output_path}")

    return df

def main():
    parser = argparse.ArgumentParser(description="Summarize ACI raster by RM for a given year")
    parser.add_argument("--year", type=int, required=True, help="Year of ACI raster to process")
    args = parser.parse_args()

    year = args.year

    # load reference data and raster
    aci_lut, muni_gdf, _, aci_path = load_data(year)

    # summarize raster by RM
    df = summarize_aci_by_rm(aci_path, muni_gdf, aci_lut, year)

    total_acres = df["acres"].sum()

    print(f"Completed ACI summary for {year}. {len(df)} records written.")
    print(f"Total compiled acreage: {total_acres:,.2f} acres")

if __name__ == "__main__":
    main()
