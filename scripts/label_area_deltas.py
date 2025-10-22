import sys
import os
import argparse
import pandas as pd
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from ag_res import paths


def load_data(year: int):
    aci_path = paths.interim(year) / f"aci_reallocated_with_pct_{year}.csv"
    masc_path = paths.interim(year) / f"masc_imputed_{year}.csv"
    lut_path = paths.reference() / "crop_label_lut.csv"

    aci_df = pd.read_csv(aci_path)
    masc_df = pd.read_csv(masc_path)
    lut_df = pd.read_csv(lut_path)

    print(f"Loaded {len(aci_df)} ACI rows for {year}")
    print(f"Loaded {len(masc_df)} MASC rows for {year}")
    return aci_df, masc_df, lut_df


def main():
    parser = argparse.ArgumentParser(description="Top up RMxLabel deficits from donor pools: Other crops, then Pasture/forages.")
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()
    year = args.year

    aci_df, masc_df, lut_df = load_data(year)

    lut_df.columns = lut_df.columns.str.strip()
    masc_df.columns = masc_df.columns.str.strip()
    aci_df.columns = aci_df.columns.str.strip()

    lut_crop_col = [c for c in lut_df.columns if c.lower() == "crop"][0]
    lut_label_col = [c for c in lut_df.columns if c.lower() == "label"][0]

    masc_lab = masc_df.merge(lut_df[[lut_crop_col, lut_label_col]],
                             left_on="crop", right_on=lut_crop_col, how="left")
    masc_lab = masc_lab.rename(columns={lut_label_col: "Label"}).drop(columns=[lut_crop_col])

    masc_group = masc_lab.groupby(["rm", "Label"], as_index=False)["acres"].sum().rename(columns={"acres": "masc_acres"})
    aci_group = aci_df.groupby(["rm", "Label"], as_index=False)["acres"].sum().rename(columns={"acres": "aci_acres"})

    df = pd.merge(aci_group, masc_group, on=["rm", "Label"], how="outer")
    df["aci_acres"] = df["aci_acres"].fillna(0.0)
    df["masc_acres"] = df["masc_acres"].fillna(0.0)

    df["aci_acres_before"] = df["aci_acres"]
    df["taken_from_other"] = 0.0
    df["taken_from_pasture"] = 0.0

    out_rows = []
    for rm, sub in df.groupby("rm", sort=False):
        sub = sub.copy()

        # Ensure donor columns exist
        if "taken_from_other" not in sub.columns:
            sub["taken_from_other"] = 0.0
        if "taken_from_pasture" not in sub.columns:
            sub["taken_from_pasture"] = 0.0

        def get_surplus(label):
            s = sub.loc[sub["Label"] == label]
            if s.empty:
                return 0.0
            return max(0.0, float(s["aci_acres"].values[0] - s["masc_acres"].values[0]))

        def take(label, amount):
            idx = sub.index[sub["Label"] == label]
            if len(idx) == 0 or amount <= 0:
                return 0.0
            i = idx[0]
            can_give = max(0.0, sub.at[i, "aci_acres"] - sub.at[i, "masc_acres"])
            give = min(amount, can_give)
            if give > 0:
                sub.at[i, "aci_acres"] -= give
            return give

        available_other = get_surplus("Other crops")
        available_pasture = get_surplus("Pasture/forages")
        available_canola = get_surplus("Canola/rapeseed")

        recipients_idx = sub.index[(sub["masc_acres"] > sub["aci_acres"])]
        for i in recipients_idx:
            need = float(sub.at[i, "masc_acres"] - sub.at[i, "aci_acres"])
            if need <= 0:
                continue

            # take from Other crops first
            give_other = take("Other crops", need)
            if give_other > 0:
                sub.at[i, "aci_acres"] += give_other
                sub.at[i, "taken_from_other"] += give_other
                need -= give_other
                available_other -= give_other

            # then Pasture/forages
            if need > 0:
                give_past = take("Pasture/forages", need)
                if give_past > 0:
                    sub.at[i, "aci_acres"] += give_past
                    sub.at[i, "taken_from_pasture"] += give_past
                    need -= give_past
                    available_pasture -= give_past

            # finally Canola/rapeseed
            if need > 0:
                give_can = take("Canola/rapeseed", need)
                if give_can > 0:
                    if "taken_from_canola" not in sub.columns:
                        sub["taken_from_canola"] = 0.0
                    sub.at[i, "aci_acres"] += give_can
                    sub.at[i, "taken_from_canola"] += give_can
                    need -= give_can
                    available_canola -= give_can

        out_rows.append(sub)

    res = pd.concat(out_rows, ignore_index=True)
    res["delta_acres_after"] = res["masc_acres"] - res["aci_acres"]
    res["status_after"] = "balanced"
    res.loc[res["delta_acres_after"] > 0, "status_after"] = "recipient"
    res.loc[res["delta_acres_after"] < 0, "status_after"] = "donor"

    cols = [
        "rm",
        "Label",
        "aci_acres_before",
        "masc_acres",
        "aci_acres",
        "delta_acres_after",
        "taken_from_other",
        "taken_from_pasture",
        "status_after",
    ]
    rest = [c for c in res.columns if c not in cols]
    res = res[cols + rest]

    out_dir = paths.interim(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"label_area_deltas_{year}.csv"
    res.to_csv(out_path, index=False)

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
