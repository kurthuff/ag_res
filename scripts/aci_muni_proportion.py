import sys
import os
import argparse
import pandas as pd
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths
from ag_res.resolve_rm_names import resolve_rm_names


def main():
    parser = argparse.ArgumentParser(description="Compute MUNI-within-RM label proportions from reallocated ACI summary.")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year

    aci_path = paths.interim(year) / f"aci_summary_reallocated_{year}.csv"
    lut_path = paths.reference() / "muni_rm_lut.csv"
    masc_path = paths.interim(year) / f"masc_imputed_{year}.csv"
    out_with_pct = paths.interim(year) / f"aci_reallocated_with_pct_{year}.csv"
    out_audit = paths.reports() / "muni_rm_pct_summaries" / f"aci_rm_label_pct_summary_{year}.csv"

    df_aci = pd.read_csv(aci_path)
    df_lut = pd.read_csv(lut_path)
    df_masc = pd.read_csv(masc_path)

    masc_rms = set(df_masc["rm"].unique())
    df_lut = resolve_rm_names(df_lut, masc_rms)
    df_lut = df_lut.drop_duplicates(subset="MUNI_NAME", keep="first")

    df = df_aci.merge(df_lut[["MUNI_NAME", "Risk Area / R.M."]], on="MUNI_NAME", how="left")
    df = df.rename(columns={"Risk Area / R.M.": "rm"})

    if df["rm"].isna().any():
        unmapped = df.loc[df["rm"].isna(), "MUNI_NAME"].drop_duplicates().tolist()
        Path("outputs/reports").mkdir(parents=True, exist_ok=True)
        pd.Series(unmapped).to_csv("outputs/reports/unmapped_munis.csv", index=False)
        print(f"Note: {len(unmapped)} municipalities not found in LUT or MASC; written to unmapped_munis.csv")
        df = df.dropna(subset=["rm"])

    grp = df.groupby(["rm", "Label"], as_index=False)["pixel_count"].sum()
    grp = grp.rename(columns={"pixel_count": "rm_label_pixels_total"})
    df = df.merge(grp, on=["rm", "Label"], how="left", validate="m:1")

    df["rm_label_pct"] = 0.0
    mask = df["rm_label_pixels_total"] > 0
    df.loc[mask, "rm_label_pct"] = df.loc[mask, "pixel_count"] / df.loc[mask, "rm_label_pixels_total"]

    df["year"] = year

    cols = [
        "year",
        "rm",
        "MUNI_NAME",
        "Label",
        "Code",
        "pixel_count",
        "rm_label_pixels_total",
        "rm_label_pct",
        "acres",
    ]
    rest = [c for c in df.columns if c not in cols]
    df = df[cols + rest]

    df.to_csv(out_with_pct, index=False)

    audit = df.groupby(["rm", "Label"]).agg(
        total_pixels=("rm_label_pixels_total", "first"),
        rows=("rm_label_pct", "size"),
        sum_pct=("rm_label_pct", "sum")
    ).reset_index()
    audit["deviation_abs"] = (audit["sum_pct"] - 1.0).abs()
    audit.to_csv(out_audit, index=False)

    print(f"Wrote {out_with_pct}")
    print(f"Wrote {out_audit}")


if __name__ == "__main__":
    main()