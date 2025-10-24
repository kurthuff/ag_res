import sys
import os
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths


def load_data(year: int):
    aci_path = paths.interim(year) / f"aci_masc_merged_{year}.csv"
    deltas_path = paths.interim(year) / f"label_area_deltas_{year}.csv"
    masc_imputed_path = paths.interim(year) / f"masc_imputed_{year}.csv"
    rpr_saf_path = paths.reference() / "rpr_saf_masc_crop.csv"
    masc_summary_path = paths.reference() / "masc_summary.csv"

    aci_df = pd.read_csv(aci_path)
    deltas_df = pd.read_csv(deltas_path)
    masc_imputed_df = pd.read_csv(masc_imputed_path)
    rpr_saf_df = pd.read_csv(rpr_saf_path)
    masc_summary_df = pd.read_csv(masc_summary_path)

    print(f"Loaded {len(aci_df)} ACI–MASC merged rows for {year}")
    print(f"Loaded {len(deltas_df)} label area delta rows for {year}")
    print(f"Loaded {len(masc_imputed_df)} MASC-imputed rows for {year}")
    print(f"Loaded {len(rpr_saf_df)} RPR/SAF reference rows")
    print("Loaded MASC summary reference table")

    return aci_df, deltas_df, masc_imputed_df, rpr_saf_df, masc_summary_df


def main():
    parser = argparse.ArgumentParser(description="Generate yield- and biomass-per-pixel tables from corrected ACI and MASC data.")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year

    df_merged, df_deltas, masc_imputed_df, rpr_saf_df, masc_summary_df = load_data(year)

    for df in [df_merged, df_deltas, masc_imputed_df, rpr_saf_df]:
        df.columns = df.columns.str.strip()

    # Merge label deltas
    df_deltas = df_deltas[["rm", "Label", "aci_acres", "masc_acres"]]
    df = df_merged.merge(df_deltas, on=["rm", "Label"], how="left", validate="m:1")

    # Calculate distributed ACI acres and pixel counts
    df["aci_acres"] = df["aci_acres"] * df["rm_label_pct"]
    df["aci_hectares"] = df["aci_acres"] * 0.404686
    df["aci_pixels"] = df["aci_hectares"] / 0.09
    df["masc_acres"] = df["masc_acres"] * df["rm_label_pct"]

    # Impute missing or zero yield values per Label
    valid_yield = df["yield_tonnes_per_acre"].notna() & (df["yield_tonnes_per_acre"] > 0)
    median_yield = df.loc[valid_yield].groupby("Label")["yield_tonnes_per_acre"].median().to_dict()
    global_yield_median = df.loc[valid_yield, "yield_tonnes_per_acre"].median()

    def impute_yield(row):
        val = row["yield_tonnes_per_acre"]
        if pd.notna(val) and val > 0:
            return val
        return median_yield.get(row["Label"], global_yield_median)

    df["masc_yield_tonnes_per_acre"] = df.apply(impute_yield, axis=1)
    df["masc_yield_tonnes_total"] = df["masc_acres"] * df["masc_yield_tonnes_per_acre"]
    df["aci_yield_tonnes_per_pixel"] = df["masc_yield_tonnes_total"] / df["aci_pixels"]

    # Compute provincial ground-truth biomass from masc_imputed + rpr_saf
    rpr_saf_df.columns = rpr_saf_df.columns.str.lower()
    masc_imputed_df.columns = masc_imputed_df.columns.str.lower()

    masc_bio_df = masc_imputed_df.merge(
        rpr_saf_df, left_on="crop", right_on="crop", how="left"
    )
    masc_bio_df["rpr"] = masc_bio_df["rpr"].fillna(1)
    masc_bio_df["saf"] = masc_bio_df["saf"].fillna(1)
    masc_bio_df["biomass_tonnes"] = masc_bio_df["yield"] * masc_bio_df["rpr"] * masc_bio_df["saf"]

    masc_biomass_tonnes_total = masc_bio_df["biomass_tonnes"].sum()

    # Impute missing biomass values by Label using median logic
    valid_biomass = df["biomass_total_muni"].notna() & (df["biomass_total_muni"] > 0)
    median_biomass = df.loc[valid_biomass].groupby("Label")["biomass_total_muni"].median().to_dict()
    global_biomass_median = df.loc[valid_biomass, "biomass_total_muni"].median()

    def impute_biomass(row):
        val = row["biomass_total_muni"]
        if pd.notna(val) and val > 0:
            return val
        return median_biomass.get(row["Label"], global_biomass_median)

    df["masc_biomass_tonnes_total"] = df.apply(impute_biomass, axis=1)
    df["aci_biomass_tonnes_per_pixel"] = df["masc_biomass_tonnes_total"] / df["aci_pixels"]

    # Remove zero-acre records before normalization
    zero_mask = (df["aci_acres"] == 0) & (df["masc_acres"] == 0)
    removed = zero_mask.sum()
    df = df.loc[~zero_mask].copy()

    # Remove infinities and NaNs
    df.replace([float("inf"), -float("inf")], 0, inplace=True)
    df.fillna(0, inplace=True)

    print(f"Removed {removed} zero-acre rows before normalization")

    # Normalize to ground truths
    masc_yield_tonnes_total = masc_summary_df.loc[
        masc_summary_df["year"] == year, "yield_tonnes"
    ].iloc[0]
    yield_factor = masc_yield_tonnes_total / df["masc_yield_tonnes_total"].sum()
    biomass_factor = masc_biomass_tonnes_total / df["masc_biomass_tonnes_total"].sum()

    df["gt_masc_yield_tonnes_total"] = df["masc_yield_tonnes_total"] * yield_factor
    df["gt_aci_yield_tonnes_per_pixel"] = df["gt_masc_yield_tonnes_total"] / df["aci_pixels"]

    df["gt_masc_biomass_tonnes_total"] = df["masc_biomass_tonnes_total"] * biomass_factor
    df["gt_aci_biomass_tonnes_per_pixel"] = df["gt_masc_biomass_tonnes_total"] / df["aci_pixels"]

    print(f"Normalized yield and biomass for {year}")
    print(f"  Yield normalization factor: {yield_factor:.6f}")
    print(f"  Biomass normalization factor: {biomass_factor:.6f}")
    print(f"  MASC ground-truth yield total: {masc_yield_tonnes_total:,.2f} tonnes")
    print(f"  MASC ground-truth biomass total: {masc_biomass_tonnes_total:,.2f} tonnes")

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
        "masc_biomass_tonnes_total",
        "aci_biomass_tonnes_per_pixel",
        "gt_masc_biomass_tonnes_total",
        "gt_aci_biomass_tonnes_per_pixel",
    ]

    df_out = df[cols]

    out_dir = paths.processed(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"aci_biomass_per_pixel_{year}.csv"

    df_out.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")

   
    log_path = paths.reports() / "biomass_normalization_log.csv"

    log_row = pd.DataFrame([{
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "year": year,
        "yield_factor": round(yield_factor, 6),
        "biomass_factor": round(biomass_factor, 6),
        "masc_yield_tonnes_total": round(masc_yield_tonnes_total, 2),
        "masc_biomass_tonnes_total": round(masc_biomass_tonnes_total, 2),
        "rows_in": len(df),
    }])

    if log_path.exists():
        log_row.to_csv(log_path, mode="a", header=False, index=False)
    else:
        log_row.to_csv(log_path, mode="w", header=True, index=False)

    print(f"Appended normalization summary to {log_path}")


if __name__ == "__main__":
    main()
