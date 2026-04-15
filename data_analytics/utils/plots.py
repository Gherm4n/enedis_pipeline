import matplotlib.pyplot as plt
import notebooks.config.matplotlib_config

import pandas as pd
import polars as pl
import polars.selectors as cs

import seaborn as sns
import notebooks.config.seaborn_config

from src.config import FIGURES_DIR


def plot_time_serie(df, timestamp_col, time_serie_title, *columns_to_filter_by):
    columns_category = []
    for column in columns_to_filter_by:
        columns_category.append(df.select(cs.contains(column)).columns)

    fig, ax = plt.subplots(nrows=columns_category.__len__(), figsize=(18, 12))

    timestamps = df.select(timestamp_col).to_numpy().ravel()

    for i, columns in enumerate(columns_category):
        for column in columns:
            sns.lineplot(x=timestamps, y=df.select(column).to_numpy().ravel(), ax=ax[i], label=column, alpha=0.6)

            ax[i].set_title(f"{columns_to_filter_by[i]}")
            ax[i].legend(loc="upper right")

    fig.suptitle(f"{time_serie_title}")
    fig.savefig(FIGURES_DIR / f"{time_serie_title}")
    plt.show()

def plot_time_serie_monthy(df, timestamp_col, time_serie_title, *column_to_filter_by):
    for month in range(1, 13):
        df_month = df.filter(pl.col("month_utc").eq(month))
        if df_month.height != 0:
            plot_time_serie(df_month, timestamp_col, time_serie_title + f"-{month}", *column_to_filter_by)

def detect_outlier():
    pass