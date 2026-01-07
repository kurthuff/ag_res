# scripts/raster_build_normalized.py

"""
Optional cartographic post-processing step.

Reduces visual artifacts at RM boundaries by normalizing per-pixel biomass
values for over-classified crops. Randomly removes pixels from statistical 
outlier RMs to bring per-pixel values to provincial median.

Input: Base rasters from raster_build.py
Output: Normalized rasters in rasters/<year>/normalized/

Preserves all MASC ground truth totals at RM level and higher.
"""

import sys
import os
import argparse
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths


def load_data(year: int):
    """Load required data for normalization"""
    biomass_path = paths.processed(year) / f"aci_biomass_per_pixel_{year}.csv"
    code_lut_path = paths.reference() / "aci_crop_classifications_iac_classifications_des_cultures.csv"
    muni_path = paths.reference() / "municipalities.geojson"
    muni_rm_lut_path = paths.reference() / "muni_rm_lut.csv"
    
    raster_codes_path = paths.rasters(year) / f"biomass_codes_{year}.tif"
    raster_values_path = paths.rasters(year) / f"biomass_values_{year}.tif"
    
    biomass_df = pd.read_csv(biomass_path)
    code_lut = pd.read_csv(code_lut_path)
    muni_gdf = gpd.read_file(muni_path)
    muni_rm_lut = pd.read_csv(muni_rm_lut_path)
    
    print(f"Loaded {len(biomass_df)} biomass per-pixel records")
    print(f"Base rasters: {raster_codes_path.name}, {raster_values_path.name}")
    
    return biomass_df, code_lut, muni_gdf, muni_rm_lut, raster_codes_path, raster_values_path


def identify_low_tail_rms(biomass_df, min_pixels, target_percentile):
    """
    Identify RMs with per-pixel biomass below target percentile for major crops.
    Aggregates all municipalities within each RM FIRST to avoid duplicates.
    """
    results = []
    
    for label in biomass_df['Label'].unique():
        label_data = biomass_df[biomass_df['Label'] == label].copy()
        
        # CRITICAL: Aggregate all MUNI_NAMEs within each RM FIRST
        rm_data = label_data.groupby('rm').agg({
            'aci_pixels': 'sum',
            'gt_masc_biomass_tonnes_total': 'sum'
        }).reset_index()
        rm_data['Label'] = label
        
        # Skip minor crops
        total_pixels = rm_data['aci_pixels'].sum()
        if total_pixels < min_pixels:
            continue
        
        # Calculate per-pixel biomass for each RM (now properly aggregated)
        rm_data['biomass_per_pixel'] = (
            rm_data['gt_masc_biomass_tonnes_total'] / rm_data['aci_pixels']
        )
        
        # Calculate target value
        target_value = rm_data['biomass_per_pixel'].quantile(target_percentile / 100)
        
        # Identify RMs below target (low tail = over-classified)
        low_tail = rm_data[rm_data['biomass_per_pixel'] < target_value].copy()
        low_tail['target_per_pixel'] = target_value
        low_tail['provincial_median'] = rm_data['biomass_per_pixel'].median()
        low_tail['provincial_pixels'] = total_pixels
        
        results.append(low_tail)
    
    if len(results) == 0:
        return pd.DataFrame()
    
    problem_df = pd.concat(results, ignore_index=True)
    
    return problem_df[['rm', 'Label', 'aci_pixels', 'gt_masc_biomass_tonnes_total',
                        'biomass_per_pixel', 'target_per_pixel', 'provincial_median',
                        'provincial_pixels']]


def calculate_pixel_removal(problem_df, max_reduction_pct):
    """
    Calculate how many pixels to remove to bring per-pixel biomass to target.
    MASC biomass total stays constant; fewer pixels = higher per-pixel value.
    """
    results = []
    
    for idx, row in problem_df.iterrows():
        # Target: biomass_total / new_pixels = target_per_pixel
        # Solve: new_pixels = biomass_total / target_per_pixel
        target_pixels = row['gt_masc_biomass_tonnes_total'] / row['target_per_pixel']
        pixels_to_remove = row['aci_pixels'] - target_pixels
        reduction_pct = (pixels_to_remove / row['aci_pixels']) * 100
        
        # Skip if exceeds safety limit
        if reduction_pct > max_reduction_pct:
            continue
        
        # Calculate new per-pixel value after removal
        new_per_pixel = row['gt_masc_biomass_tonnes_total'] / target_pixels
        
        results.append({
            'rm': row['rm'],
            'Label': row['Label'],
            'current_pixels': int(row['aci_pixels']),
            'target_pixels': int(target_pixels),
            'pixels_to_remove': int(pixels_to_remove),
            'reduction_pct': reduction_pct,
            'current_per_pixel': row['biomass_per_pixel'],
            'target_per_pixel': row['target_per_pixel'],
            'new_per_pixel': new_per_pixel,
            'masc_biomass_total': row['gt_masc_biomass_tonnes_total']
        })
    
    return pd.DataFrame(results)


def get_rm_mask(rm_name, muni_gdf, muni_rm_lut, raster_crs, transform, shape):
    """Create a boolean mask for a given RM"""
    from rasterio.features import rasterize
    
    # Get municipalities in this RM
    muni_names = muni_rm_lut[muni_rm_lut['Risk Area / R.M.'] == rm_name]['MUNI_NAME'].tolist()
    
    if len(muni_names) == 0:
        return np.zeros(shape, dtype=bool)
    
    # Get geometries and reproject to raster CRS
    rm_geoms_gdf = muni_gdf[muni_gdf['MUNI_NAME'].isin(muni_names)].copy()
    
    if rm_geoms_gdf.crs != raster_crs:
        rm_geoms_gdf = rm_geoms_gdf.to_crs(raster_crs)
    
    # Rasterize to mask
    mask = rasterize(
        [(geom, 1) for geom in rm_geoms_gdf.geometry],
        out_shape=shape,
        transform=transform,
        fill=0,
        dtype='uint8'
    )
    
    return mask.astype(bool)


def remove_pixels_randomly(raster_codes, raster_values, rm_mask, label_code, 
                           n_to_remove, masc_biomass_total, seed=None):
    """
    Randomly remove pixels by setting them to nodata.
    Recalculate per-pixel biomass for remaining pixels to preserve MASC total.
    """
    if seed is not None:
        np.random.seed(seed)
    
    # Find all pixels of this crop in this RM
    eligible_mask = rm_mask & (raster_codes == label_code)
    eligible_pixels = np.where(eligible_mask)
    eligible_indices = list(zip(eligible_pixels[0], eligible_pixels[1]))
    
    if len(eligible_indices) == 0:
        return raster_codes, raster_values, 0, 0
    
    if len(eligible_indices) < n_to_remove:
        n_to_remove = len(eligible_indices)
    
    # Randomly select pixels to remove
    remove_indices = np.random.choice(
        len(eligible_indices), 
        size=n_to_remove, 
        replace=False
    )
    
    # Remove selected pixels (set to nodata)
    for pixel_idx in remove_indices:
        row, col = eligible_indices[pixel_idx]
        raster_codes[row, col] = 0
        raster_values[row, col] = -9999.0
    
    # Count remaining pixels
    remaining_pixels = np.sum(rm_mask & (raster_codes == label_code))
    
    # Recalculate per-pixel value for remaining pixels
    if remaining_pixels > 0:
        new_per_pixel_value = masc_biomass_total / remaining_pixels
        update_mask = rm_mask & (raster_codes == label_code)
        raster_values[update_mask] = new_per_pixel_value
    else:
        new_per_pixel_value = 0
    
    return raster_codes, raster_values, n_to_remove, new_per_pixel_value


def main():
    parser = argparse.ArgumentParser(
        description="Cartographic normalization of biomass rasters"
    )
    parser.add_argument("--year", type=int, required=True,
                       help="Year of rasters to normalize")
    parser.add_argument("--target-percentile", type=int, default=40,
                       help="Target percentile for normalization (default: 50)")
    parser.add_argument("--min-pixels", type=int, default=5000,
                       help="Minimum provincial pixels to consider a crop major (default: 10000)")
    parser.add_argument("--max-reduction", type=float, default=30,
                       help="Maximum percentage of pixels to remove per RM×Label (default: 50)")
    parser.add_argument("--seed", type=int, default=2024,
                       help="Random seed for reproducibility (default: 2024)")
    
    args = parser.parse_args()
    year = args.year
    
    print("=" * 70)
    print("CARTOGRAPHIC PIXEL NORMALIZATION")
    print("=" * 70)
    print(f"Year: {year}")
    print(f"Target: {args.target_percentile}th percentile")
    print(f"Major crops threshold: {args.min_pixels:,} pixels")
    print(f"Maximum reduction: {args.max_reduction}%")
    print(f"Random seed: {args.seed}")
    print()
    
    # Load data
    biomass_df, code_lut, muni_gdf, muni_rm_lut, codes_path, values_path = load_data(year)
    
    # Step 1: Identify low-tail RMs for major crops
    print("Identifying low-tail RMs...")
    problem_rms = identify_low_tail_rms(biomass_df, args.min_pixels, args.target_percentile)
    
    if len(problem_rms) == 0:
        print("No problematic RMs identified. Exiting.")
        return
    
    print(f"  Found {len(problem_rms)} problematic RM × Label combinations")
    print(f"  Crops affected: {problem_rms['Label'].nunique()}")
    print(f"  Unique RMs: {problem_rms['rm'].nunique()}")
    
    # Step 2: Calculate removal targets
    print("\nCalculating pixel removal targets...")
    targets = calculate_pixel_removal(problem_rms, args.max_reduction)
    
    if len(targets) == 0:
        print(f"  No adjustments within {args.max_reduction}% safety limit. Exiting.")
        return
    
    print(f"  {len(targets)} adjustments planned (after {args.max_reduction}% limit)")
    print(f"  Total pixels to remove: {targets['pixels_to_remove'].sum():,}")
    print(f"  Average reduction: {targets['reduction_pct'].mean():.1f}%")
    
    # Export targets for review
    reports_dir = paths.reports() / "normalization"
    reports_dir.mkdir(parents=True, exist_ok=True)
    targets_path = reports_dir / f"pixel_removal_targets_{year}.csv"
    targets.to_csv(targets_path, index=False)
    print(f"\n  Targets saved to: {targets_path}")
    
    # Summary by crop
    print("\n  Removal Summary by Crop:")
    crop_summary = targets.groupby('Label').agg({
        'pixels_to_remove': 'sum',
        'reduction_pct': 'mean',
        'rm': 'count'
    }).rename(columns={'rm': 'num_rms'})
    for label, row in crop_summary.iterrows():
        print(f"    {label}: {row['pixels_to_remove']:,.0f} pixels from {row['num_rms']:.0f} RMs "
              f"(avg {row['reduction_pct']:.1f}%)")
    
    # Confirmation
    print("\n" + "=" * 70)
    response = input("Proceed with pixel removal? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted.")
        return
    
    # Step 3: Load rasters
    print("\nLoading rasters...")
    with rasterio.open(codes_path) as src:
        codes_array = src.read(1).copy()
        codes_profile = src.profile.copy()
        codes_transform = src.transform
        codes_shape = codes_array.shape
        codes_crs = src.crs
    
    with rasterio.open(values_path) as src:
        values_array = src.read(1).copy()
        values_profile = src.profile.copy()
    
    print(f"  Raster shape: {codes_shape}")
    print(f"  Raster CRS: {codes_crs}")
    print(f"  Initial non-zero pixels: {np.sum(codes_array > 0):,}")
    
    # Step 4: Apply corrections
    print("\nApplying pixel removals...")
    adjustments_log = []
    
    for idx, row in targets.iterrows():
        rm = row['rm']
        label = row['Label']
        code_match = code_lut[code_lut['Label'] == label]
        
        if len(code_match) == 0:
            print(f"  Warning: No code found for {label}, skipping")
            continue
        
        code = int(code_match['Code'].iloc[0])
        n_remove = int(row['pixels_to_remove'])
        
        # Get RM mask
        rm_mask = get_rm_mask(rm, muni_gdf, muni_rm_lut, codes_crs, codes_transform, codes_shape)
        
        if not rm_mask.any():
            print(f"  Warning: Empty mask for {rm}, skipping")
            continue
        
        # Remove pixels
        codes_array, values_array, n_removed, new_value = remove_pixels_randomly(
            codes_array, values_array, rm_mask, code, n_remove,
            row['masc_biomass_total'],
            seed=args.seed + idx
        )
        
        adjustments_log.append({
            'rm': rm,
            'Label': label,
            'Code': code,
            'pixels_removed': n_removed,
            'old_pixel_count': row['current_pixels'],
            'new_pixel_count': row['target_pixels'],
            'old_per_pixel': row['current_per_pixel'],
            'new_per_pixel': new_value,
            'target_per_pixel': row['target_per_pixel'],
            'reduction_pct': row['reduction_pct'],
            'masc_biomass_total': row['masc_biomass_total']
        })
        
        if (idx + 1) % 10 == 0:
            print(f"  Processed {idx + 1}/{len(targets)} adjustments...")
    
    print(f"  Completed {len(adjustments_log)} adjustments")
    print(f"  Final non-zero pixels: {np.sum(codes_array > 0):,}")
    
    # Step 5: Write normalized rasters
    print("\nWriting normalized rasters...")
    out_dir = paths.rasters(year) / "normalized"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    out_codes = out_dir / f"biomass_codes_{year}.tif"
    out_values = out_dir / f"biomass_values_{year}.tif"
    
    with rasterio.open(out_codes, 'w', **codes_profile) as dst:
        dst.write(codes_array, 1)
    
    with rasterio.open(out_values, 'w', **values_profile) as dst:
        dst.write(values_array, 1)
    
    print(f"  Codes: {out_codes}")
    print(f"  Values: {out_values}")
    
    # Step 6: Save adjustment log
    adj_df = pd.DataFrame(adjustments_log)
    log_path = reports_dir / f"pixel_removals_applied_{year}.csv"
    adj_df.to_csv(log_path, index=False)
    print(f"  Log: {log_path}")
    
    # Final summary
    print("\n" + "=" * 70)
    print("NORMALIZATION COMPLETE")
    print("=" * 70)
    
    if len(adj_df) == 0:
        print("No adjustments were made.")
        print("All RMs are within acceptable ranges or no major crops fell below threshold.")
        print(f"\nNormalized rasters written (identical to base): {out_dir}")
        print(f"Reports: {reports_dir}")
    else:
        print(f"Total pixels removed: {adj_df['pixels_removed'].sum():,}")
        print(f"Average per-pixel improvement: {adj_df['new_per_pixel'].mean() - adj_df['old_per_pixel'].mean():.6f} tonnes")
        print(f"RMs corrected: {adj_df['rm'].nunique()}")
        print(f"Crops corrected: {adj_df['Label'].nunique()}")
        print(f"\nMASC biomass in adjusted RMs: {adj_df['masc_biomass_total'].sum():,.2f} tonnes (preserved exactly)")
        print(f"\nNormalized rasters: {out_dir}")
        print(f"Reports: {reports_dir}")


if __name__ == "__main__":
    main()