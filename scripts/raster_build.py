import sys
import os
import re
import argparse
import rasterio
import numpy as np
import pandas as pd
import geopandas as gpd
from rasterio.features import geometry_window, geometry_mask
from shapely.geometry import mapping
from pathlib import Path
from scipy.ndimage import gaussian_filter

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
    """Load all reference and per-pixel biomass data."""
    aci_csv = paths.processed(year) / f"aci_biomass_per_pixel_{year}.csv"
    crop_ref = paths.reference() / "aci_crop_classifications_iac_classifications_des_cultures.csv"
    muni_path = paths.reference() / "municipalities.geojson"
    prov_path = paths.reference() / "Manitoba_Provincial_Boundary_2830875166235622803.geojson"

    df = pd.read_csv(aci_csv)
    df_ref = pd.read_csv(crop_ref)
    gdf_muni = gpd.read_file(muni_path)
    gdf_prov = gpd.read_file(prov_path)

    df["Label"] = df["Label"].astype(str).str.strip()
    df_ref["Label"] = df_ref["Label"].astype(str).str.strip()

    label_to_code = df_ref.set_index("Label")["Code"].to_dict()
    return df, gdf_muni, gdf_prov, label_to_code


def assign_within_muni(band1, band2, df_muni, label_to_code, protected_codes, rng, valid_mask=None):
    """Assign biomass directly per RM×Label record, using its own biomass value."""
    h, w = band1.shape
    assigned_mask = np.zeros((h, w), dtype=bool)
    assigned_log = []

    # Rule 1 – preserve existing matching codes
    for lbl, sub in df_muni.groupby("Label"):
        code = label_to_code.get(lbl)
        if code is None or pd.isna(code):
            continue
        code = int(code)
        biomass_val = float(sub["gt_aci_biomass_tonnes_per_pixel"].iloc[0])
        mask = band1 == code
        if np.any(mask):
            band2[mask] = biomass_val
            assigned_mask[mask] = True

    # Rule 2 – fill deficits using the per-record biomass value
    for _, row in df_muni.iterrows():
        lbl = str(row["Label"]).strip()
        code = label_to_code.get(lbl)
        if code is None or pd.isna(code):
            continue
        code = int(code)
        required_pixels = int(round(row.get("aci_pixels", 0)))
        if required_pixels <= 0:
            continue

        biomass_val = float(row["gt_aci_biomass_tonnes_per_pixel"])
        mask_existing = band1 == code
        current = int(np.count_nonzero(mask_existing))
        deficit = required_pixels - current
        if deficit <= 0:
            continue

        cand_mask = (~assigned_mask) & (~np.isin(band1, list(protected_codes)))
        if valid_mask is not None:
            cand_mask &= valid_mask

        candidates = np.argwhere(cand_mask)
        if candidates.size == 0:
            continue

        deficit = min(deficit, len(candidates))
        idx = rng.choice(len(candidates), size=deficit, replace=False)
        rows, cols = candidates[idx].T
        band1[rows, cols] = code
        band2[rows, cols] = biomass_val
        assigned_mask[rows, cols] = True

        assigned_log.append({
            "MUNI_NAME": row["MUNI_NAME"],
            "Label": lbl,
            "Code": code,
            "required_pixels": required_pixels,
            "existing_pixels": current,
            "newly_assigned": int(deficit),
            "total_assigned": int(current + deficit),
            "biomass_value": biomass_val
        })

    band2 = np.where((band2 < 0) & (band2 != -9999.0), 0, band2)
    return band1, band2, assigned_log


def process_by_municipality(src, dst_codes, dst_biomass,
                            df, gdf_muni, gdf_prov,
                            label_to_code, rng, only_muni=None):
    protected_codes = {10, 20, 30, 34, 35, 50, 60, 80, 85, 110, 130, 200, 210, 220, 230}
    logs = []

    gdf_muni = gdf_muni.to_crs(src.crs)
    gdf_prov = gdf_prov.to_crs(src.crs)

    manitoba_union = gdf_prov.union_all()
    gdf_muni = gdf_muni.clip(manitoba_union)

    for _, muni in gdf_muni.iterrows():
        muni_name = muni["MUNI_NAME"]
        if only_muni and muni_name != only_muni:
            continue
        df_muni = df[df["MUNI_NAME"] == muni_name]
        if df_muni.empty:
            continue

        geom = [mapping(muni.geometry)]
        try:
            window = geometry_window(src, geom, pad_x=0, pad_y=0, north_up=True)
        except Exception:
            continue

        band1_chunk = src.read(1, window=window)
        if band1_chunk.size == 0 or np.all(band1_chunk == 0):
            print(f"Skipping {muni_name}: no overlapping pixels")
            continue

        mask_geom = geometry_mask(
            geometries=geom,
            out_shape=(window.height, window.width),
            transform=src.window_transform(window),
            invert=True
        )

        band2_chunk = np.full_like(band1_chunk, -9999.0, dtype="float32")
        band1_chunk, band2_chunk, assigned_log = assign_within_muni(
            band1_chunk, band2_chunk, df_muni, label_to_code,
            protected_codes, rng, valid_mask=mask_geom
        )

        dst_codes.write(band1_chunk.astype("uint16"), 1, window=window)
        dst_biomass.write(band2_chunk.astype("float32"), 1, window=window)

        for rec in assigned_log:
            rec["MUNI_NAME"] = muni_name
        logs.extend(assigned_log)
        print(f"Processed {muni_name}: {len(assigned_log)} label rows")

    return logs

from scipy.ndimage import gaussian_filter

def create_smoothed_copy(raster_path: Path, sigma: float = 1.0):
    """Create a Gaussian-smoothed raster without diluting across nodata areas."""
    smooth_path = raster_path.with_name(raster_path.stem + "_smoothed.tif")

    with rasterio.open(raster_path) as src:
        arr = src.read(1).astype("float32")
        nodata = src.nodata
        valid_mask = (arr != nodata) & ~np.isnan(arr)

        data = np.where(valid_mask, arr, 0)
        weights = valid_mask.astype("float32")

        # Weighted Gaussian filter (avoid nodata bleeding)
        smooth_data = gaussian_filter(data, sigma=sigma)
        smooth_weights = gaussian_filter(weights, sigma=sigma)

        with np.errstate(invalid="ignore", divide="ignore"):
            smoothed = smooth_data / smooth_weights

        smoothed[~valid_mask] = nodata

        profile = src.profile
        with rasterio.open(smooth_path, "w", **profile) as dst:
            dst.write(smoothed.astype("float32"), 1)

    print(f"Smoothed raster written: {smooth_path}")
    return smooth_path

def write_diagnostics(year: int, records: list):
    out_dir = paths.outputs() / "reports" / "raster_build"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"raster_assignment_summary_{year}.csv"
    pd.DataFrame(records).to_csv(out_path, index=False)
    print(f"Wrote diagnostic report: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Build biomass rasters directly from per-RM per-Label values.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--muni", type=str, help="Optional: process only one municipality for testing")
    args = parser.parse_args()
    year = args.year
    only_muni = args.muni

    aci_dir = paths.raw(year)
    aci_path = choose_aci_for_year(aci_dir, year)
    print(f"Using ACI raster: {aci_path.name}")

    df, gdf_muni, gdf_prov, label_to_code = load_data(year)
    rng = np.random.default_rng()

    out_dir = paths.outputs() / "rasters" / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_code_path = out_dir / f"biomass_codes_{year}.tif"
    out_bio_path = out_dir / f"biomass_values_{year}.tif"

    with rasterio.open(aci_path) as src:
        profile_codes = src.profile.copy()
        profile_bio = src.profile.copy()

        profile_codes.update({
            "count": 1, "dtype": "uint16", "nodata": 0, "compress": "LZW"
        })
        profile_bio.update({
            "count": 1, "dtype": "float32", "nodata": -9999.0,
            "compress": "LZW", "predictor": 2
        })

        with rasterio.open(out_code_path, "w", **profile_codes) as dst_codes, \
             rasterio.open(out_bio_path, "w", **profile_bio) as dst_biomass:

            dst_codes.write(np.zeros((src.height, src.width), dtype="uint16"), 1)
            dst_biomass.write(np.full((src.height, src.width), -9999.0, dtype="float32"), 1)

            logs = process_by_municipality(
                src, dst_codes, dst_biomass,
                df, gdf_muni, gdf_prov, label_to_code, rng, only_muni
            )

    write_diagnostics(year, logs)
    print(f"Rasters complete for {year}:")
    print(f"  Codes → {out_code_path}")
    print(f"  Biomass → {out_bio_path}")

    # to output a smoothed raster as well, uncomment the line below.
    # create_smoothed_copy(out_bio_path, sigma=1.0)


if __name__ == "__main__":
    main()
