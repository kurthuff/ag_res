import pandas as pd

def resolve_rm_names(df_lut: pd.DataFrame, masc_rms: set) -> pd.DataFrame:
    df = df_lut.copy()

    # mapping of ambiguous MUNI_NAME to possible RM candidates
    rules = {
        "MUNICIPALITY OF ROBLIN": ["HILLSBURG-ROBLIN-SHELL RIVER", "ROBLIN"],
        "MUNICIPALITY OF WESTLAKE-GLADSTONE": ["WESTBOURNE", "GLADSTONE"],
        "MUNICIPALITY OF KILLARNEY-TURTLE MOUNTAIN": ["KILLARNEY-TURTLE MOUNTAIN", "TURTLE MOUNTAIN"],
    }

    def pick(muni, rm):
        if muni in rules:
            for candidate in rules[muni]:
                if candidate in masc_rms:
                    return candidate
        return rm

    df["Risk Area / R.M."] = df.apply(lambda x: pick(x["MUNI_NAME"], x["Risk Area / R.M."]), axis=1)

    return df