import sys
import os
import argparse
import pandas as pd
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths


def load_data(year: int):
    masc_path = paths.interim(year) / f"masc_imputed_{year}.csv"
    aci_path = paths.interim(year) / f"aci_reallocated_with_pct_{year}.csv"
    crop_label_lut_path = paths.reference() / "crop_label_lut.csv"
    rpr_saf_path = paths.reference() / "rpr_saf_masc_crop.csv"

    masc_df = pd.read_csv(masc_path)
    aci_df = pd.read_csv(aci_path)
    lut_df = pd.read_csv(crop_label_lut_path)
    rpr_saf_df = pd.read_csv(rpr_saf_path)

    print(f"Loaded {len(masc_df)} MASC-imputed rows for {year}")
    print(f"Loaded {len(aci_df)} ACI proportion rows for {year}")
    print(f"Loaded {len(rpr_saf_df)} RPR/SAF reference rows")

    return masc_df, aci_df, lut_df, rpr_saf_df


def main():
    parser = argparse.ArgumentParser(description="Merge ACI proportions with MASC yield and biomass data by RM and Label.")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year

    masc_df, aci_df, lut_df, rpr_saf_df = load_data(year)

    out_merged = paths.interim(year) / f"aci_masc_merged_{year}.csv"
    out_audit = paths.reports() / "aci_masc_merge" / f"aci_masc_merge_summary_{year}.csv"

    for df in [lut_df, masc_df, aci_df, rpr_saf_df]:
        df.columns = df.columns.str.strip()

    lut_crop_col = [c for c in lut_df.columns if c.lower() == "crop"][0]
    lut_label_col = [c for c in lut_df.columns if c.lower() == "label"][0]
    rpr_crop_col = [c for c in rpr_saf_df.columns if c.lower() == "crop"][0]

    masc_df = masc_df.merge(rpr_saf_df, left_on="crop", right_on=rpr_crop_col, how="left")
    masc_df = masc_df.merge(lut_df[[lut_crop_col, lut_label_col]], left_on="crop", right_on=lut_crop_col, how="left")

    masc_df = masc_df.rename(columns={lut_label_col: "Label"})
    for c in [lut_crop_col, rpr_crop_col]:
        if c in masc_df.columns:
            masc_df = masc_df.drop(columns=c)

    masc_df["RPR"] = masc_df["RPR"].fillna(1)
    masc_df["SAF"] = masc_df["SAF"].fillna(1)
    masc_df["biomass_tonnes"] = masc_df["yield"] * masc_df["RPR"] * masc_df["SAF"]

    masc_group = masc_df.groupby(["rm", "Label"], as_index=False).agg(
        acres_masc=("acres", "sum"),
        yield_tonnes_per_acre=("yield_per_acre", "mean"),
        yield_total=("yield", "sum"),
        biomass_total=("biomass_tonnes", "sum")
    )

    df = aci_df.merge(masc_group, on=["rm", "Label"], how="left")

    df["yield_tonnes_per_acre_muni"] = df["yield_tonnes_per_acre"] * df["rm_label_pct"]
    df["yield_total_muni"] = df["yield_total"] * df["rm_label_pct"]
    df["biomass_total_muni"] = df["biomass_total"] * df["rm_label_pct"]

    df[["yield_tonnes_per_acre_muni", "yield_total_muni", "biomass_total_muni"]] = df[
        ["yield_tonnes_per_acre_muni", "yield_total_muni", "biomass_total_muni"]
    ].fillna(0)

    df.to_csv(out_merged, index=False)

    audit = df.groupby(["rm", "Label"]).agg(
        masc_acres=("acres_masc", "first"),
        aci_acres=("acres", "sum"),
        sum_yield_total_muni=("yield_total_muni", "sum"),
        mean_yield_tonnes_per_acre=("yield_tonnes_per_acre_muni", "mean"),
        sum_biomass_total_muni=("biomass_total_muni", "sum")
    ).reset_index()

    audit.to_csv(out_audit, index=False)

    print(f"Wrote {out_merged}")
    print(f"Wrote {out_audit}")


if __name__ == "__main__":
    main()
