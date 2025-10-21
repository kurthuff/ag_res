import sys
import os
import argparse
from pathlib import Path
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths


def load_data(year: int):
    imputed_path = paths.interim(year) / f"masc_imputed_{year}.csv"
    aci_summary_path = paths.interim(year) / f"aci_summary_{year}.csv"
    crop_label_lut_path = paths.reference() / "crop_label_lut.csv"
    muni_rm_lut_path = paths.reference() / "muni_rm_lut.csv"

    masc_df = pd.read_csv(imputed_path)
    aci_df = pd.read_csv(aci_summary_path)
    cl_lut_df = pd.read_csv(crop_label_lut_path)
    mr_lut_df = pd.read_csv(muni_rm_lut_path)

    print(f"Loaded {len(masc_df)} MASC-imputed rows for {year}")
    print(f"Loaded {len(aci_df)} ACI RM-summary rows for {year}")

    return masc_df, aci_df, cl_lut_df, mr_lut_df


def preprocess_data(masc_df, aci_df, cl_lut_df, mr_lut_df):
    # merge municipality names to RM names
    aci_df = aci_df.merge(mr_lut_df[["MUNI_NAME", "Risk Area / R.M."]], on="MUNI_NAME", how="left")
    aci_df = aci_df.rename(columns={"Risk Area / R.M.": "rm"})

    # collapse ACI to RM × Label level
    aci_grouped = (
        aci_df.groupby(["rm", "Label"], as_index=False)[["pixel_count", "hectares", "acres"]].sum()
    )

    # link MASC crop names to ACI labels
    masc_mapped = masc_df.merge(cl_lut_df, left_on="crop", right_on="Crop", how="left")

    # collapse MASC to RM × Label level
    masc_grouped = (
        masc_mapped.groupby(["rm", "Label"], as_index=False)["acres"].sum()
        .rename(columns={"acres": "acres_masc"})
    )

    # merge for comparison
    merged = aci_grouped.merge(masc_grouped, on=["rm", "Label"], how="outer").fillna(0)
    merged = merged.rename(columns={"acres": "acres_aci"})
    merged["acres_diff"] = merged["acres_aci"] - merged["acres_masc"]

    return merged, aci_df


def reallocate_acres(df):
    results = []

    for rm, rm_df in df.groupby("rm"):
        rm_df = rm_df.copy()

        # Case 1: ACI > 0, MASC = 0 → remove ACI-only crops
        mask_case1 = (rm_df["acres_aci"] > 0) & (rm_df["acres_masc"] == 0)
        rm_df.loc[mask_case1, ["acres_aci", "pixel_count", "hectares"]] = 0

        # Case 2: ACI = 0, MASC > 0 → missing MASC crops
        missing = rm_df[(rm_df["acres_aci"] == 0) & (rm_df["acres_masc"] > 0)]
        surplus = rm_df[(rm_df["acres_aci"] > rm_df["acres_masc"])].copy()

        if not missing.empty and not surplus.empty:
            # prioritize crops with the largest positive surplus
            surplus = surplus.sort_values("acres_diff", ascending=False)

            for idx, row in missing.iterrows():
                target_add = row["acres_masc"]

                for jdx, s_row in surplus.iterrows():
                    if target_add <= 0:
                        break
                    available = s_row["acres_diff"]
                    if available <= 0:
                        continue

                    take = min(available, target_add)
                    rm_df.loc[rm_df.index == jdx, "acres_aci"] -= take
                    rm_df.loc[rm_df.index == idx, "acres_aci"] += take
                    target_add -= take
                    surplus.loc[jdx, "acres_diff"] -= take

            rm_df["acres_diff"] = rm_df["acres_aci"] - rm_df["acres_masc"]

        results.append(rm_df)

    return pd.concat(results, ignore_index=True)


def distribute_back_to_municipalities(aci_df, reallocated_df):
    # attach rm to ensure proper join context
    if "rm" not in aci_df.columns:
        raise ValueError("aci_df is missing 'rm' column. Ensure preprocess_data merged the RM lookup.")

    # ensure all RM–Label combos exist in aci_df
    existing_keys = set(zip(aci_df["rm"], aci_df["Label"]))
    template_cols = ["MUNI_NO", "MUNI_NAME", "Code", "pixel_count", "hectares", "acres", "Label", "rm"]
    new_rows = []

    for rm, label, acres_aci in reallocated_df[["rm", "Label", "acres_aci"]].itertuples(index=False):
        if (rm, label) not in existing_keys:
            template = aci_df.loc[aci_df["rm"] == rm]
            if not template.empty:
                template_row = template.iloc[0]
                new_rows.append({
                    "MUNI_NO": template_row["MUNI_NO"],
                    "MUNI_NAME": template_row["MUNI_NAME"],
                    "Code": template_row["Code"],
                    "pixel_count": 0.0,
                    "hectares": 0.0,
                    "acres": 0.0,
                    "Label": label,
                    "rm": rm,
                })
            else:
                new_rows.append({
                    "MUNI_NO": None,
                    "MUNI_NAME": rm,
                    "Code": None,
                    "pixel_count": 0.0,
                    "hectares": 0.0,
                    "acres": 0.0,
                    "Label": label,
                    "rm": rm,
                })

    if new_rows:
        aci_df = pd.concat([aci_df, pd.DataFrame(new_rows)[template_cols]], ignore_index=True)

    # 1. sum original acres by rm + Label
    orig_totals = (
        aci_df.groupby(["rm", "Label"], as_index=False)["acres"]
        .sum()
        .rename(columns={"acres": "acres_orig"})
    )

    # 2. get new totals from reallocated_df
    new_totals = reallocated_df[["rm", "Label", "acres_aci"]].copy()

    # 3. merge new totals into the original table
    df = aci_df.merge(orig_totals, on=["rm", "Label"], how="left")
    df = df.merge(new_totals, on=["rm", "Label"], how="left")

    # 4. compute scaling factor
    df["scale_factor"] = df.apply(
        lambda r: r["acres_aci"] / r["acres_orig"]
        if r["acres_orig"] > 0 and pd.notnull(r["acres_aci"])
        else (1.0 if pd.isnull(r["acres_aci"]) else 0.0),
        axis=1,
    )

    # 5. rescale pixel_count, hectares, acres
    df["pixel_count"] = df["pixel_count"] * df["scale_factor"]
    df["hectares"] = df["hectares"] * df["scale_factor"]
    df["acres"] = df["acres"] * df["scale_factor"]

    # 5b. fill values for new crops (acres_orig == 0 but acres_aci > 0)
    mask_new = (df["acres_orig"] == 0) & (df["acres_aci"] > 0)
    df.loc[mask_new, "acres"] = df.loc[mask_new, "acres_aci"]
    df.loc[mask_new, "hectares"] = df.loc[mask_new, "acres"] * 0.404685642
    df.loc[mask_new, "pixel_count"] = df.loc[mask_new, "hectares"] / 0.09

    # 6. restore column order
    final = df[["MUNI_NO", "MUNI_NAME", "Code", "pixel_count", "hectares", "acres", "Label"]]

    return final


def summarize_changes(before_df: pd.DataFrame, after_df: pd.DataFrame, year: int):
    # before_df must have: rm, Label, acres_aci, acres_masc
    # after_df  must have: rm, Label, acres_aci (post-reallocation)
    compare_cols = ["rm", "Label", "acres_aci", "acres_masc"]
    before = before_df[compare_cols].rename(columns={"acres_aci": "acres_before"})
    after = after_df[["rm", "Label", "acres_aci"]].rename(columns={"acres_aci": "acres_after"})

    joined = before.merge(after, on=["rm", "Label"], how="outer").fillna(0)

    joined["delta_acres"] = joined["acres_after"] - joined["acres_before"]
    joined["diff_vs_masc_before"] = joined["acres_before"] - joined["acres_masc"]
    joined["diff_vs_masc_after"] = joined["acres_after"] - joined["acres_masc"]

    changed = joined.loc[joined["delta_acres"].abs() > 0]

    summary = (
        changed.groupby("rm", as_index=False)
        .agg(
            changed_crops=("Label", "count"),
            total_delta_acres=("delta_acres", "sum"),
            mean_diff_vs_masc_before=("diff_vs_masc_before", "mean"),
            mean_diff_vs_masc_after=("diff_vs_masc_after", "mean"),
        )
        .sort_values("changed_crops", ascending=False)
    )

    detail_path = Path("outputs") / "reports" / "reallocation" / f"aci_reallocation_changes_{year}_detail.csv"
    summary_path = Path("outputs") / "reports" / "reallocation" / f"aci_reallocation_changes_{year}_summary.csv"
    detail_path.parent.mkdir(parents=True, exist_ok=True)

    changed.to_csv(detail_path, index=False)
    summary.to_csv(summary_path, index=False)

    print(f"Saved before/after change report: {detail_path}")
    print(f"Saved per-RM summary: {summary_path}")

def main():
    parser = argparse.ArgumentParser(description="Reallocate ACI crop acres based on MASC-imputed ground truth")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year

    masc_df, aci_df, cl_lut_df, mr_lut_df = load_data(year)
    merged_df, aci_df = preprocess_data(masc_df, aci_df, cl_lut_df, mr_lut_df)

    total_aci_before = merged_df["acres_aci"].sum()
    total_masc = merged_df["acres_masc"].sum()
    diff_before = total_aci_before - total_masc

    reallocated_df = reallocate_acres(merged_df)

    total_aci_after = reallocated_df["acres_aci"].sum()
    diff_after = total_aci_after - total_masc

    num_case1 = ((merged_df["acres_aci"] > 0) & (merged_df["acres_masc"] == 0)).sum()
    num_case2 = ((merged_df["acres_aci"] == 0) & (merged_df["acres_masc"] > 0)).sum()

     # write change reports at RM × Label level
    summarize_changes(merged_df, reallocated_df, year)

    # distribute back to municipalities with original structure
    final_df = distribute_back_to_municipalities(aci_df, reallocated_df)

    output_path = paths.interim(year) / f"aci_summary_reallocated_{year}.csv"
    final_df.to_csv(output_path, index=False)
    print(f"Saved reallocated file: {output_path}")

    stats_row = {
        "year": year,
        "aci_total_before": total_aci_before,
        "masc_total": total_masc,
        "diff_before": diff_before,
        "aci_total_after": total_aci_after,
        "diff_after": diff_after,
        "case1_removed": int(num_case1),
        "case2_added": int(num_case2),
    }

    stats_path = Path("outputs") / "reports" / "reallocation" / "aci_reallocation_stats.csv"
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    if stats_path.exists():
        stats_df = pd.read_csv(stats_path)
        stats_df = pd.concat([stats_df, pd.DataFrame([stats_row])], ignore_index=True)
    else:
        stats_df = pd.DataFrame([stats_row])
    stats_df.to_csv(stats_path, index=False)
    print(f"Appended summary statistics to {stats_path}")


if __name__ == "__main__":
    main()
