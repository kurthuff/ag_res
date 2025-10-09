import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
import argparse

# project convention for relative paths
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths

# calamine neccessary to properly load .xlsx from MASC website
def load_data(year: int):
    raw_path = paths.raw(year) / f"masc_{year}.xlsx"
    summary_path = paths.raw(year).parent / "masc_summary.csv"
    masc_df = pd.read_excel(raw_path, engine="calamine")
    summary_df = pd.read_csv(summary_path)
    return masc_df, summary_df

# cleaning the raw .xlsx from MASC
def clean_masc(df: pd.DataFrame):
    df = df.drop(0).reset_index(drop=True)
    df.columns = [
        "year",
        "rm",
        "crop",
        "variety",
        "farms",
        "acres",
        "yield_per_acre",
        "metric_unit",
        "imperial_value",
        "imperial_unit",
    ]
    df = df[["year", "rm", "crop", "variety", "farms", "acres", "yield_per_acre"]]
    df["yield_per_acre"] = df["yield_per_acre"].str.split().str[0]
    df["yield_per_acre"] = pd.to_numeric(df["yield_per_acre"], errors="coerce")
    df["farms"] = pd.to_numeric(df["farms"], errors="coerce").astype("Int64")
    df["acres"] = df["acres"].astype(str).str.replace(",", "", regex=False)
    df["acres"] = pd.to_numeric(df["acres"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["yield"] = df["acres"] * df["yield_per_acre"]
    return df

# masc_summary.csv has been built manually from the search summary section of the 
# MASC search results. It's neccesary to perform imputation as it contains ground truth
# of crop agreage and yield per acre in Manitoba annually. Search summary acreage and 
# dervied yield is compared to .xlsx results, which contain a large percentage of suppressed
# results, making an imputation scheme necessary. 
def compute_diffs(masc_df, summary_df, year):
    acres_sum = masc_df["acres"].sum()
    summary_acres_sum = summary_df.loc[summary_df["year"] == year, "total_acres"].iloc[0]
    acres_diff = summary_acres_sum - acres_sum
    yield_sum = masc_df["yield"].sum()
    summary_total_acres = summary_df.loc[summary_df["year"] == year, "total_acres"].iloc[0]
    summary_yield_per_acre = summary_df.loc[
        summary_df["year"] == year, "yield_tonnes_per_acre"
    ].iloc[0]
    summary_yield_sum = summary_total_acres * summary_yield_per_acre
    yield_diff = summary_yield_sum - yield_sum
    print(f"Acres difference: {acres_diff:.2f}")
    print(f"Yield difference: {yield_diff:.2f}")
    return acres_diff, yield_diff

# valid_df informs calculated totals on missing_df
def split_valid_missing(masc_df):
    missing_df = masc_df[
        masc_df["yield_per_acre"].isna()
        | masc_df["farms"].isna()
        | masc_df["acres"].isna()
    ].copy()
    valid_df = masc_df[
        masc_df["farms"].notna()
        & masc_df["acres"].notna()
        & masc_df["yield_per_acre"].notna()
    ].copy()
    print(f"Valid rows: {len(valid_df)}")
    print(f"Missing rows: {len(missing_df)}")
    return valid_df, missing_df

# proxy_map maps all crops to staple crops, which are guaranteed, due to thier high production,
# to never be suppressed in MASC data, and therefore will always have numeric values by which
# to inform the imputation strategy. If a crop exists in valid_df, it's own numeric values
# overwrite any mapping to a staple crop. Therefore, the proxy_map acts as a backup strategy so that
# no crop-varieties in missing_df with less than 3 farms or 500 acres (making them suppressed for privacy)
# will be left out of the final imputation.
#
# To simplify imputation, and backed up by analysis found in notebooks/masc_eda.ipynb, each instance
# in missing_df, which represents a crop-variety for a specific rm, will be assumed to be 1 farm.
# See notebooks/masc_eda.ipynb for details on average acreage per crop per farm found from valid_df
# values, which shows that each farm for all crops but one has an average acreage <500 acres, aligning with
# the criteria for suppression in the first place.
def impute(missing_df, valid_df, acres_diff, yield_diff):
    crop_stats = (
        valid_df.groupby("crop", as_index=False)
        .agg({"acres": "sum", "farms": "sum", "yield": "sum"})
    )
    crop_stats["avg_acres_per_farm"] = crop_stats["acres"] / crop_stats["farms"]
    crop_stats["avg_yield_per_farm"] = crop_stats["yield"] / crop_stats["farms"]
    crop_stats["yield_tonnes_per_acre"] = crop_stats["yield"] / crop_stats["acres"]

    proxy_map = {  # full proxy_map from notebook
        "POLISH CANOLA": "ARGENTINE CANOLA",
        "RAPESEED": "ARGENTINE CANOLA",
        "OIL SUNFLOWERS": "ARGENTINE CANOLA",
        "NON-OIL SUNFLOWERS": "ARGENTINE CANOLA",
        "HEMP GRAIN": "SOYBEANS",
        "ORGANIC HEMP GRAIN": "SOYBEANS",
        "MUSTARD": "ARGENTINE CANOLA",
        "FLAX": "ARGENTINE CANOLA",
        "ORGANIC FLAX": "ARGENTINE CANOLA",
        "PHACELIA": "ARGENTINE CANOLA",
        "RED SPRING WHEAT": "RED SPRING WHEAT",
        "NORTH. HARD RED WHT": "RED SPRING WHEAT",
        "PRAIRIE SPRING WHEAT": "RED SPRING WHEAT",
        "OTHER SPRING WHEAT": "RED SPRING WHEAT",
        "DURUM WHEAT": "RED SPRING WHEAT",
        "HARD WHITE WHEAT": "RED SPRING WHEAT",
        "EXTRA STRONG WHEAT": "RED SPRING WHEAT",
        "WINTER WHEAT": "RED SPRING WHEAT",
        "WINTER TRITICALE": "RED SPRING WHEAT",
        "TRITICALE": "RED SPRING WHEAT",
        "EMMER WHEAT": "RED SPRING WHEAT",
        "SPELT": "RED SPRING WHEAT",
        "ORGANIC DURUM WHEAT": "RED SPRING WHEAT",
        "ORGANIC E.S. WHEAT": "RED SPRING WHEAT",
        "ORGANIC H.W. WHEAT": "RED SPRING WHEAT",
        "ORGANIC N.H.R. WHT": "RED SPRING WHEAT",
        "ORGANIC P.S. WHEAT": "RED SPRING WHEAT",
        "ORGANIC R.S. WHEAT": "RED SPRING WHEAT",
        "ORGANIC SPR WHT OTHR": "RED SPRING WHEAT",
        "ORGANIC WINTER WHEAT": "RED SPRING WHEAT",
        "OPEN POL. FALL RYE": "RED SPRING WHEAT",
        "HYBRID FALL RYE": "RED SPRING WHEAT",
        "ORGANIC O P FALL RYE": "RED SPRING WHEAT",
        "OPEN POL SILAGE CORN": "GRAIN CORN",
        "OPEN POLLINATED CORN": "GRAIN CORN",
        "ORGANIC BARLEY": "BARLEY",
        "ORGANIC OATS": "OATS",
        "BARLEY": "BARLEY",
        "BUCKWHEAT": "OATS",
        "OATS": "OATS",
        "MIXED GRAIN": "BARLEY",
        "PROSO MILLET": "OATS",
        "MILLET (FOXTAIL SEED)": "OATS",
        "MILO/GRAIN SORGHUM": "GRAIN CORN",
        "SOYBEANS": "SOYBEANS",
        "PINTO BEANS": "SOYBEANS",
        "BLACK BEANS": "SOYBEANS",
        "WHITE PEA BEANS": "SOYBEANS",
        "SMALL RED BEANS": "SOYBEANS",
        "CRANBERRY BEANS": "SOYBEANS",
        "KIDNEY BEANS": "SOYBEANS",
        "OTH DRY EDIBLE BEANS": "SOYBEANS",
        "ADZUKI BEANS": "SOYBEANS",
        "FABABEANS": "FIELD PEAS",
        "FIELD PEAS": "FIELD PEAS",
        "LENTILS": "FIELD PEAS",
        "ORGANIC FIELD PEAS": "FIELD PEAS",
        "PROC POTATOES-IRRIG": "FIELD PEAS",
        "PROC POTATOES-DRYLND": "FIELD PEAS",
        "TABLE POTATOES": "FIELD PEAS",
        "SUGAR BEETS": "FIELD PEAS",
        "CARROTS": "FIELD PEAS",
        "CAULIFLOWER": "FIELD PEAS",
        "ASPARAGUS": "FIELD PEAS",
        "CUCUMBERS": "FIELD PEAS",
        "SWEET CORN": "FIELD PEAS",
        "RED BEET": "FIELD PEAS",
        "RUTABAGAS": "FIELD PEAS",
        "COOKING ONIONS": "FIELD PEAS",
        "SWEET POTATO": "FIELD PEAS",
        "CABBAGE": "FIELD PEAS",
        "PARSNIPS": "FIELD PEAS",
        "QUINOA": "FIELD PEAS",
        "ALFALFA": "COARSE HAY",
        "ALFALFA/GRASS MIX.": "COARSE HAY",
        "ALFALFA EST OPTION": "COARSE HAY",
        "ALFALFA GRASS EST OP": "COARSE HAY",
        "ALFALFA SEED EST OPT": "COARSE HAY",
        "COMMON ALFALFA SEED": "COARSE HAY",
        "PED. ALFALFA SEED": "COARSE HAY",
        "COARSE HAY": "COARSE HAY",
        "GREENFEED": "COARSE HAY",
        "FORAGE ESTABLISHMENT": "COARSE HAY",
        "CLOVER & BROMEGRASS HAY": "COARSE HAY",
        "CLOVER RD CNRYG & TIM HAY": "COARSE HAY",
        "INTERCROP MIXTURE": "COARSE HAY",
        "SWEET CLOVER": "COARSE HAY",
        "SWEET CLOVER (SEED)": "COARSE HAY",
        "SWEET CLOVER EST OPT": "COARSE HAY",
        "SAINFOIN (FORAGE)": "COARSE HAY",
        "CICER MILKVETCH (FORAGE)": "COARSE HAY",
        "HAIRY VETCH": "COARSE HAY",
        "GRASSES": "PASTURE (TAME/PERENNIAL)",
        "GRASSES (SEED)": "PASTURE (TAME/PERENNIAL)",
        "GRASSES (SOD)": "PASTURE (TAME/PERENNIAL)",
        "PASTURE (TAME/ANNUAL)": "PASTURE (TAME/PERENNIAL)",
        "PASTURE (TAME/PERENNIAL)": "PASTURE (TAME/PERENNIAL)",
        "PER. RYEGRASS SEED": "PASTURE (TAME/PERENNIAL)",
        "ANNUAL RYEGRASS SEED": "PASTURE (TAME/PERENNIAL)",
        "TALL FESCUE SEED": "PASTURE (TAME/PERENNIAL)",
        "TIMOTHY SEED COMMON": "PASTURE (TAME/PERENNIAL)",
        "PED. TIMOTHY SEED": "PASTURE (TAME/PERENNIAL)",
        "ALSIKE CLOVER (FORAGE)": "PASTURE (TAME/PERENNIAL)",
        "ALSIKE CLOVER (SEED)": "PASTURE (TAME/PERENNIAL)",
        "BIRDSFOOT TREFOIL (SEED)": "PASTURE (TAME/PERENNIAL)",
        "BIRDSFOOT TREFOIL FORAGE": "PASTURE (TAME/PERENNIAL)",
        "FALL GRASS EST OPTN": "PASTURE (TAME/PERENNIAL)",
        "FALL TIMOTHY EST OPT": "PASTURE (TAME/PERENNIAL)",
        "SPRING GRASS EST OPT": "PASTURE (TAME/PERENNIAL)",
        "SPRING TIMOTHY EST O": "PASTURE (TAME/PERENNIAL)",
        "LUPINS (FORAGE)": "PASTURE (TAME/PERENNIAL)",
        "LUPINS (SEED)": "PASTURE (TAME/PERENNIAL)",
        "CARAWAY": "PASTURE (TAME/PERENNIAL)",
        "CANARYSEED": "PASTURE (TAME/PERENNIAL)",
        "SILAGE CORN": "GRAIN CORN",
        "NON-CORN SILAGE": "GRAIN CORN",
        "TOO WET TO SEED": "COARSE HAY",
        "UNUSED LAND": "COARSE HAY",
        "BUSH/YARD/SLOUGH": "COARSE HAY",
    }

    missing_df["crop"] = missing_df["crop"].str.strip().str.upper()
    valid_df["crop"] = valid_df["crop"].str.strip().str.upper()

    unmatched_crops = set(missing_df["crop"].unique()) - set(valid_df["crop"].unique())
    missing_df.loc[missing_df["crop"].isin(unmatched_crops), "crop"] = (
        missing_df.loc[missing_df["crop"].isin(unmatched_crops), "crop"].replace(proxy_map)
    )

    stable_crops = crop_stats[crop_stats["acres"] >= 10000]["crop"].tolist()
    fallback_acres = crop_stats.loc[
        crop_stats["crop"].isin(stable_crops), "avg_acres_per_farm"
    ].median()
    fallback_yield = crop_stats.loc[
        crop_stats["crop"].isin(stable_crops), "avg_yield_per_farm"
    ].median()

    missing_counts = (
        missing_df.groupby("crop", as_index=False)
        .size()
        .rename(columns={"size": "n_missing"})
    )
    crop_stats = crop_stats.merge(missing_counts, on="crop", how="left").fillna({"n_missing": 0})
    crop_stats = crop_stats[crop_stats["n_missing"] > 0].copy()
    crop_stats["avg_acres_per_farm"] = crop_stats["avg_acres_per_farm"].fillna(fallback_acres)
    crop_stats["avg_yield_per_farm"] = crop_stats["avg_yield_per_farm"].fillna(fallback_yield)

    crop_stats["weight_acres"] = crop_stats["avg_acres_per_farm"] * crop_stats["n_missing"]
    crop_stats["weight_norm_acres"] = crop_stats["weight_acres"] / crop_stats["weight_acres"].sum()
    crop_stats["crop_imputed_total_acres"] = crop_stats["weight_norm_acres"] * acres_diff

    crop_stats["weight_yield"] = crop_stats["avg_yield_per_farm"] * crop_stats["n_missing"]
    crop_stats["weight_norm_yield"] = crop_stats["weight_yield"] / crop_stats["weight_yield"].sum()
    crop_stats["crop_imputed_total_yield"] = crop_stats["weight_norm_yield"] * yield_diff

    missing_df = missing_df.merge(
        crop_stats[
            ["crop", "n_missing", "crop_imputed_total_acres", "crop_imputed_total_yield"]
        ],
        on="crop",
        how="left",
    )

    missing_df["imputed_acres"] = missing_df["crop_imputed_total_acres"] / missing_df["n_missing"]
    missing_df["imputed_yield_tonnes"] = (
        missing_df["crop_imputed_total_yield"] / missing_df["n_missing"]
    )

    print("Acreage check:", missing_df["imputed_acres"].sum(), "expected:", acres_diff)
    print("Yield check:", missing_df["imputed_yield_tonnes"].sum(), "expected:", yield_diff)

    valid_df["imputed"] = 0
    missing_df["imputed"] = 1
    missing_df["acres"] = missing_df["imputed_acres"]
    missing_df["farms"] = 1
    missing_df["yield"] = missing_df["imputed_yield_tonnes"].astype("Float64")
    missing_df["yield_per_acre"] = missing_df["yield"] / missing_df["acres"]

    cols_to_drop = [
        "n_missing",
        "crop_imputed_total_acres",
        "crop_imputed_total_yield",
        "imputed_acres",
        "imputed_yield_tonnes",
    ]
    missing_df = missing_df.drop(columns=cols_to_drop)

    recombined_df = pd.concat([valid_df, missing_df], ignore_index=True)
    return recombined_df


def main(year):
    masc_raw, summary_df = load_data(year)
    masc_df = clean_masc(masc_raw)
    acres_diff, yield_diff = compute_diffs(masc_df, summary_df, year)
    valid_df, missing_df = split_valid_missing(masc_df)
    final_df = impute(missing_df, valid_df, acres_diff, yield_diff)

    summary_row = summary_df.loc[summary_df["year"] == year].iloc[0]
    total_acres = final_df["acres"].sum()
    total_yield = final_df["yield"].sum()
    expected_yield = (
        summary_row["total_acres"] * summary_row["yield_tonnes_per_acre"]
    )
    print("\nFinal checks:")
    print(f"Total acres in recombined_df: {total_acres:.2f} vs {summary_row['total_acres']:.2f}")
    print(f"Total yield in recombined_df: {total_yield:.2f} vs {expected_yield:.2f}")

    out_path = paths.interim(year) / f"masc_imputed_{year}.csv"
    final_df.to_csv(out_path, index=False)
    print(f"Saved imputed file: {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    main(args.year)
