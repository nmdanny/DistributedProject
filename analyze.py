import pandas as pd
import numpy as np
from pathlib import Path
from ast import literal_eval
from scipy.stats import ks_2samp

DECODES_PATHS = [Path("out") / f"{i}-decode.csv" for i in [0]]

SHARES_PATHS = [Path("out") / f"{i}-share.csv" for i in range(0, 3)]


RAND_PATH = Path("out") / "1337-share.csv"

def get_shares_df() -> pd.DataFrame:
    dfs = []
    for shares_path in SHARES_PATHS:
        df = pd.read_csv(shares_path)
        dfs.append(df)
    df = pd.concat(dfs)
    df = convert_shares_df(df)

    df['share_px'] = df['share_px'].apply(lambda x: int.from_bytes(literal_eval(x), byteorder='little', signed=False))

    rand_df = pd.read_csv(RAND_PATH)
    rand_df['share_px'] = rand_df['share_px'].apply(lambda x: int.from_bytes(literal_eval(x), byteorder='little', signed=False))



    return df, rand_df

def analyze_encodes() -> pd.DataFrame:
    dfs = []
    for decode_path in DECODES_PATHS:
        df = pd.read_csv(decode_path)
        dfs.append(df)
    df = pd.concat(dfs)

    df['malformed'] = df['malformed'].astype(int)
    print(df.describe())
    return df

def convert_shares_df(df: pd.DataFrame) -> pd.DataFrame:
    return df

if __name__ == "__main__":
    analyze_encodes()

    df, rand_df = get_shares_df()

    stat = ks_2samp(df['share_px'], rand_df['share_px'])

    print("As statistic -> 0 or pvalue -> 1, the samples are more likely to be from equal distribution")
    print("As statistic -> 1 or pvalue -> 0, the samples are more likely to be from different distribution")
    print("Kolmogorov-Smirnov statistic:", stat)


