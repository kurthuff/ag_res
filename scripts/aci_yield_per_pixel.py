import sys
import os
import argparse
import pandas as pd
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths


def load_data(year: int):
    aci_path = paths.interim(year) / f"aci_masc_merged_{year}.csv"
    deltas_path = paths.interim(year) / f"label_area_deltas_{year}.csv"
    masc_summary_path = paths.reference() / "masc_summary.csv"

    aci_df = pd.read_csv(aci_path)
    deltas_df = pd.read_csv(deltas_path)
    masc_summary_df = pd.read_csv(masc_summary_path)

    print(f"Loaded {len(aci_df)} ACI–MASC merged rows for {year}")
    print(f"Loaded {len(deltas_df)} label area delta rows for {year}")
    print(f"Loaded MASC summary reference table")

    return aci_df, deltas_df, masc_summary_df


def main():
    parser = argparse.ArgumentParser(description="Generate yield-per-pixel table from corrected ACI and MASC data.")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year

    df_merged, df_deltas, masc_summary_df = load_data(year)

    df_merged.columns = df_merged.columns.str.strip()
    df_deltas.columns = df_deltas.columns.str.strip()

    # Select only required fields from deltas
    df_deltas = df_deltas[["rm", "Label", "aci_acres", "masc_acres"]]

    # Merge RM×Label totals into RM×MUNI_NAME×Label detail
    df = df_merged.merge(df_deltas, on=["rm", "Label"], how="left", validate="m:1")

    # Calculate distributed ACI acres
    df["aci_acres"] = df["aci_acres"] * df["rm_label_pct"]

    # Derived unit conversions
    df["aci_hectares"] = df["aci_acres"] * 0.404686
    df["aci_pixels"] = df["aci_hectares"] / 0.09  # 30 m pixels → 0.09 ha

   # Distributed MASC acres and yield
    df["masc_acres"] = df["masc_acres"] * df["rm_label_pct"]

    # Impute missing or zero yields per Label using median of valid (nonzero, non-NaN) values
    valid = df["yield_tonnes_per_acre"].notna() & (df["yield_tonnes_per_acre"] > 0)
    median_yields = (
        df.loc[valid]
        .groupby("Label")["yield_tonnes_per_acre"]
        .median()
        .to_dict()
    )

    # Global fallback median if a Label has no valid nonzero entries
    global_median = df.loc[valid, "yield_tonnes_per_acre"].median()

    def impute_yield(row):
        val = row["yield_tonnes_per_acre"]
        if pd.notna(val) and val > 0:
            return val
        return median_yields.get(row["Label"], global_median)

    df["masc_yield_tonnes_per_acre"] = df.apply(impute_yield, axis=1)
    df["masc_yield_tonnes_total"] = df["masc_acres"] * df["masc_yield_tonnes_per_acre"]

    # Yield per pixel
    df["aci_yield_tonnes_per_pixel"] = df["masc_yield_tonnes_total"] / df["aci_pixels"]

    # Normalize to MASC ground truth totals
    target_yield = masc_summary_df.loc[masc_summary_df["year"] == year, "yield_tonnes"].iloc[0]
    current_yield = df["masc_yield_tonnes_total"].sum()
    gt_factor = target_yield / current_yield

    df["gt_masc_yield_tonnes_total"] = df["masc_yield_tonnes_total"] * gt_factor
    df["gt_aci_yield_tonnes_per_pixel"] = df["gt_masc_yield_tonnes_total"] / df["aci_pixels"]

    print(f"Normalized to ground truth for {year}")
    print(f"  Target yield (MASC): {target_yield:,.2f} tonnes")
    print(f"  Before normalization: {current_yield:,.2f} tonnes")
    print(f"  Applied factor: {gt_factor:.6f}")
    print(f"  After normalization: {df['gt_masc_yield_tonnes_total'].sum():,.2f} tonnes")

    # Columns for final output including ground-truth normalized fields
    cols = [
        "year",
        "rm",
        "MUNI_NAME",
        "MUNI_NO",
        "Label",
        "Code",
        "aci_acres",
        "aci_hectares",
        "aci_pixels",
        "masc_acres",
        "masc_yield_tonnes_per_acre",
        "masc_yield_tonnes_total",
        "aci_yield_tonnes_per_pixel",
        "gt_masc_yield_tonnes_total",
        "gt_aci_yield_tonnes_per_pixel",
    ]

    df_out = df[cols]

    # Output path
    out_dir = paths.processed(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"aci_yield_per_pixel_{year}.csv"

    df_out.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
