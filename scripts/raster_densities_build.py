import sys
import os
import argparse
import numpy as np
import rasterio
from rasterio.windows import Window
from scipy.signal import fftconvolve

# project paths
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths


def make_circular_kernel(radius_px):
    y, x = np.ogrid[-radius_px:radius_px+1, -radius_px:radius_px+1]
    return ((x**2 + y**2) <= radius_px**2).astype(np.float32)


def process_tile_total_tonnes(band, mask, kernel):
    # band units: tonnes per pixel
    # biomass_sum: total tonnes in the radius (sum of tonnes/pixel)
    biomass_sum = fftconvolve(np.where(np.isfinite(band), band, 0.0), kernel, mode="same")
    valid_sum = fftconvolve(mask.astype(np.float32), kernel, mode="same")
    full_count = float(kernel.sum())
    with np.errstate(divide="ignore", invalid="ignore"):
        # extrapolate to full circle if partially outside valid area
        scaled_tonnes = np.where(valid_sum > 0, biomass_sum * (full_count / valid_sum), np.nan)
    return scaled_tonnes.astype(np.float32)


def main():
    parser = argparse.ArgumentParser(description="Total biomass within 1 km radius per pixel (tonnes).")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--radius_m", type=float, default=1000.0)
    parser.add_argument("--tile", type=int, default=2048)
    args = parser.parse_args()

    year = args.year
    radius_m = args.radius_m
    tile_size = args.tile

    in_dir = paths.outputs() / "rasters" / str(year)
    in_path = in_dir / f"biomass_values_{year}.tif"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing input raster: {in_path}")

    out_path = in_dir / f"biomass_densities_{year}.tif"

    # radius in pixels (assumes ~30 m)
    with rasterio.open(in_path) as src_probe:
        px_m = float(src_probe.res[0])
    radius_px = int(round(radius_m / px_m))
    overlap = radius_px + 2
    kernel = make_circular_kernel(radius_px)

    with rasterio.open(in_path) as src:
        profile = src.profile.copy()
        profile.update(dtype="float32", count=1, nodata=-9999.0, compress="LZW", predictor=2)

        with rasterio.open(out_path, "w", **profile) as dst:
            n_rows, n_cols = src.height, src.width

            for row_start in range(0, n_rows, tile_size):
                for col_start in range(0, n_cols, tile_size):
                    row_off = max(row_start - overlap, 0)
                    col_off = max(col_start - overlap, 0)
                    nrows = min(tile_size + 2 * overlap, n_rows - row_off)
                    ncols = min(tile_size + 2 * overlap, n_cols - col_off)

                    win_read = Window(col_off, row_off, ncols, nrows)
                    band = src.read(1, window=win_read).astype(np.float32)

                    # valid: everything that is not nodata and finite
                    nodata_in = src.nodata
                    if nodata_in is None:
                        mask = np.isfinite(band)
                    else:
                        mask = np.isfinite(band) & (band != nodata_in)

                    total_full = process_tile_total_tonnes(band, mask, kernel)

                    inner_r0 = overlap if row_start > 0 else 0
                    inner_c0 = overlap if col_start > 0 else 0
                    inner_r1 = inner_r0 + min(tile_size, n_rows - row_start)
                    inner_c1 = inner_c0 + min(tile_size, n_cols - col_start)

                    total_crop = total_full[inner_r0:inner_r1, inner_c0:inner_c1]
                    total_crop = np.where(np.isfinite(total_crop), total_crop, -9999.0).astype(np.float32)

                    win_write = Window(col_start, row_start, total_crop.shape[1], total_crop.shape[0])
                    dst.write(total_crop, 1, window=win_write)

                print(f"Processed row block {row_start} / {n_rows}")

    print(f"wrote {out_path}")
    print(f"radius: {radius_m} m | kernel width: {2*radius_px+1} px | tile: {tile_size} | overlap: {overlap}")


if __name__ == "__main__":
    main()
