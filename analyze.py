import pandas as pd
import numpy as np
from pathlib import Path
from ast import literal_eval
import plotnine as p9
import matplotlib.pyplot as plt
from scipy.stats import ks_2samp


SHARES_PATHS = [Path("out") / f"{i}-share.csv" for i in range(0, 3)]


RAND_PATH = Path("out") / "1337-share.csv"
DECODES_PATHS = list(Path("out").glob("1-decode-*.csv"))

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

    df['alpha'] = df['num_channels'] / df['num_clients']


    plot_df = df.groupby('alpha').mean()
    plot_df.index.name = 'alpha'
    plot_df.reset_index(inplace=True)

    n_alphas = df['alpha'].unique()
    print(f"Using {n_alphas.shape} different alphas from {len(DECODES_PATHS)} runs")

    plot = (p9.ggplot(plot_df) + p9.aes('alpha', 'malformed') + p9.geom_point() + p9.geom_smooth(method='lm')  + p9.labels.labs(x='Alpha', y='Collision Percentage')
    )

    plot.draw()
    plt.show()


    return df

def convert_shares_df(df: pd.DataFrame) -> pd.DataFrame:
    return df

if __name__ == "__main__":
    analyze_encodes()

    df, rand_df = get_shares_df()

    stat = ks_2samp(df['share_px'], rand_df['share_px'])

    print("As statistic -> 0 or pvalue -> 1, the samples are more likely to be from equal distribution")
    print("As statistic -> 1 or pvalue -> 0, the samples are more likely to be from different distribution")
    print("Kolmogorov-Smirnov statistic when :", stat)


