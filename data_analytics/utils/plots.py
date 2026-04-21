import sys
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import notebooks.config.matplotlib_config

import numpy as np
from scipy import stats

import pandas as pd
import polars as pl
import polars.selectors as cs

import seaborn as sns
import notebooks.config.seaborn_config

from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

def plot_acf_decomposition(df, column, lags=[48, 336], save_path=None):
    """
    Plots the autocorrelation of a column for multiple lags.
    """
    # Theme colors
    BG     = '#0f172a'
    PANEL  = '#1e293b'
    TEXT   = '#e2e8f0'
    SUBTLE = '#94a3b8'

    if not df.schema[column].is_numeric():
        return

    series_data = df.select(column).drop_nulls().to_numpy().ravel()
    if len(series_data) == 0:
        return
    
    for lag_val in lags:
        prefix = "ACF" if "plot_acf_decomposition" in sys._getframe().f_code.co_name else "PACF"
        save_file = save_path / f"{prefix}_{column}_{lag_val}.png" if save_path else None
        if save_file and save_file.exists():
            continue

        fig, ax = plt.subplots(figsize=(22, 6), facecolor=BG)
        fig.suptitle(
            f"Autocorrélation — {column}",
            fontsize=16, fontweight='bold', color=TEXT, y=1.01
        )

        plot_acf(series_data, lags=lag_val, ax=ax)

        ax.set_facecolor(PANEL)
        ax.set_title(f"Fenêtre : {lag_val * 0.5}h", color=SUBTLE, fontsize=12, pad=8)

        # recolor the bars and confidence band produced by plot_acf
        for line in ax.get_lines():
            line.set_color('#3b82f6')
            line.set_alpha(0.7)
        for collection in ax.collections:
            collection.set_facecolor('#3b82f6')
            collection.set_alpha(0.15)

        ax.axhline(0, color=SUBTLE, linewidth=0.8, linestyle='--')
        ax.tick_params(colors=SUBTLE, labelsize=9)
        ax.xaxis.label.set_color(SUBTLE)
        ax.yaxis.label.set_color(SUBTLE)
        for spine in ax.spines.values():
            spine.set_edgecolor('#334155')
        ax.grid(True, color='#334155', linewidth=0.5, linestyle='--', alpha=0.6)

        fig.tight_layout()
        if save_path:
            save_path.mkdir(parents=True, exist_ok=True)
            fig.savefig(save_path / f"ACF_{column}_{lag_val}.png", facecolor=fig.get_facecolor(), bbox_inches="tight")
        plt.close(fig)

def plot_pacf_decomposition(df, column, lags=[48, 336], save_path=None):
    """
    Plots the partial autocorrelation of a column for multiple lags.
    """
    # Theme colors
    BG     = '#0f172a'
    PANEL  = '#1e293b'
    TEXT   = '#e2e8f0'
    SUBTLE = '#94a3b8'

    if not df.schema[column].is_numeric():
        return

    series_data = df.select(column).drop_nulls().to_numpy().ravel()
    if len(series_data) == 0:
        return
    
    for lag_val in lags:
        prefix = "ACF" if "plot_acf_decomposition" in sys._getframe().f_code.co_name else "PACF"
        save_file = save_path / f"{prefix}_{column}_{lag_val}.png" if save_path else None
        if save_file and save_file.exists():
            continue

        fig, ax = plt.subplots(figsize=(22, 6), facecolor=BG)
        fig.suptitle(
            f"Autocorrélation Partielle — {column}",
            fontsize=16, fontweight='bold', color=TEXT, y=1.01
        )

        plot_pacf(series_data, lags=lag_val, ax=ax)

        ax.set_facecolor(PANEL)
        ax.set_title(f"Fenêtre : {lag_val * 0.5}h", color=SUBTLE, fontsize=12, pad=8)

        for line in ax.get_lines():
            line.set_color('#f97316')
            line.set_alpha(0.7)
        for collection in ax.collections:
            collection.set_facecolor('#f97316')
            collection.set_alpha(0.15)

        ax.axhline(0, color=SUBTLE, linewidth=0.8, linestyle='--')
        ax.tick_params(colors=SUBTLE, labelsize=9)
        ax.xaxis.label.set_color(SUBTLE)
        ax.yaxis.label.set_color(SUBTLE)
        for spine in ax.spines.values():
            spine.set_edgecolor('#334155')
        ax.grid(True, color='#334155', linewidth=0.5, linestyle='--', alpha=0.6)

        fig.tight_layout()
        if save_path:
            save_path.mkdir(parents=True, exist_ok=True)
            fig.savefig(save_path / f"PACF_{column}_{lag_val}.png", facecolor=fig.get_facecolor(), bbox_inches="tight")
        plt.close(fig)

def plot_decomposition(df, column, period, save_path, title):
    """
    Plots the seasonal decomposition of a column (trend, seasonal, residuals).
    """
    save_file = save_path / f"{title}.png"
    if save_file.exists():
        return
    
    # Check if numeric
    if not df.schema[column].is_numeric():
        return

    # Drop nulls for decomposition
    series_data = df.select(column).drop_nulls()
    if series_data.height < period * 2: # Minimum data for decomposition
        return
        
    # Statsmodels expects a numpy array or pandas series
    decomp = seasonal_decompose(series_data.to_numpy().ravel(), model="additive", period=period)

    # Theme colors from notebook
    BG     = '#0f172a'
    PANEL  = '#1e293b'
    TEXT   = '#e2e8f0'
    SUBTLE = '#94a3b8'

    fig, axes = plt.subplots(nrows=3, figsize=(22, 12), facecolor=BG)
    fig.suptitle(
        f"Décomposition — {column}  ·  fenêtre {period * 0.5}h",
        fontsize=16, fontweight='bold', color=TEXT, y=0.98
    )

    series = [
        (decomp.seasonal, "Saisonnalité", '#3b82f6'),
        (decomp.trend,    "Tendance",     '#f97316'),
        (decomp.resid,    "Résidu",       '#a855f7'),
    ]

    for ax, (data, label, color) in zip(axes, series):
        ax.set_facecolor(PANEL)
        ax.plot(data, color=color, linewidth=1.2, alpha=0.9)
        ax.set_title(label, color=TEXT, fontsize=13, fontweight='bold', pad=8)

        ax.tick_params(colors=SUBTLE, labelsize=9)
        for spine in ax.spines.values():
            spine.set_edgecolor('#334155')

        ax.xaxis.label.set_color(SUBTLE)
        ax.yaxis.label.set_color(SUBTLE)
        ax.grid(True, color='#334155', linewidth=0.5, linestyle='--', alpha=0.6)

        # zero line for resid and seasonal
        if label != "Tendance":
            ax.axhline(0, color=SUBTLE, linewidth=0.8, linestyle=':')

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    
    save_path.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_file, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)


def plot_time_serie(df, timestamp_col, time_serie_title, save_path, *columns_to_filter_by):
    save_file = save_path / f"{time_serie_title}.png"
    if save_file.exists():
        return
    columns_category = []
    for column in columns_to_filter_by:
        columns_category.append(df.select(cs.contains(column)).columns)

    fig, ax = plt.subplots(nrows=columns_category.__len__(), figsize=(18, 12))

    timestamps = df.select(timestamp_col).to_numpy().ravel()

    if len(columns_category) == 1:
        sns.lineplot(
            x=timestamps,
            y=df.select(columns_category[0]).to_numpy().ravel(),
            ax=ax,
            label=columns_to_filter_by[0],
            alpha=0.6,
        )
    else:
        for i, columns in enumerate(columns_category):
            for column in columns:
                sns.lineplot(
                    x=timestamps,
                    y=df.select(column).to_numpy().ravel(),
                    ax=ax[i],
                    label=column,
                    alpha=0.6,
                )

                ax[i].set_title(f"{columns_to_filter_by[i]}")
                ax[i].legend(loc="upper right")

    fig.suptitle(f"{time_serie_title}")

    save_file.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_file)
    plt.close()


def plot_time_serie_monthly(df, timestamp_col, time_serie_title, save_path, *column_to_filter_by):
    for month in range(1, 13):
        df_month = df.filter(pl.col("month_utc").eq(month))
        if df_month.height != 0:
            plot_time_serie(
                df_month,
                timestamp_col,
                time_serie_title + f"-{month}",
                save_path,
                *column_to_filter_by,
            )


def detect_outlier(df, column, iqr_multiplier=1.5):
    """
    Detect outliers in a Polars DataFrame column using IQR and Z-score methods.
    Returns a Polars Expression for boolean mask.
    """
    if not df.schema[column].is_numeric():
        return pl.lit(False)

    col_series = df[column].drop_nulls()
    q1 = float(col_series.quantile(0.25, interpolation="nearest"))
    q3 = float(col_series.quantile(0.75, interpolation="nearest"))
    iqr = q3 - q1
    lower = q1 - iqr_multiplier * iqr
    upper = q3 + iqr_multiplier * iqr

    # IQR mask
    iqr_mask = (pl.col(column) < lower) | (pl.col(column) > upper)

    # Z-score mask (Z > 3)
    # Note: Using standard deviation and mean for z-score
    mean = float(col_series.mean())
    std = float(col_series.std())
    z_mask = ((pl.col(column) - mean).abs() / std) > 3

    return iqr_mask | z_mask


def plot_distribution(
    df, column, time_col, iqr_multiplier=1.5, figsize=(16, 10), save_path=None
):
    """
    Plots the distribution of a column, including time series with outlier overlay,
    histogram, box plot, and Q-Q plot.
    """
    if save_path.exists():
        return
    # ── Validation ────────────────────────────────────────────────────────────
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame.")
    
    # Check if numeric - distribution plots (hist, box, qq) require numeric data
    if not df.schema[column].is_numeric():
        print(f"Skipping distribution plot for non-numeric column: {column}")
        return

    # Polars: drop_nulls on Series, convert to numpy for scipy/matplotlib
    col_series = df[column].drop_nulls()
    values_np = col_series.to_numpy()  # 1-D numpy array — no index

    # ── Stats ─────────────────────────────────────────────────────────────────
    mean = float(col_series.mean())
    median = float(col_series.median())
    std = float(col_series.std())
    q1 = float(col_series.quantile(0.25, interpolation="nearest"))
    q3 = float(col_series.quantile(0.75, interpolation="nearest"))
    iqr = q3 - q1
    lower = q1 - iqr_multiplier * iqr
    upper = q3 + iqr_multiplier * iqr

    z_scores = np.abs(stats.zscore(values_np))
    mask_iqr = (values_np < lower) | (values_np > upper)
    outliers_iqr = values_np[mask_iqr]
    outliers_z = values_np[z_scores > 3]
    outlier_pct = len(outliers_iqr) / len(values_np) * 100
    skewness = float(stats.skew(values_np))
    kurtosis = float(stats.kurtosis(values_np))

    # keep series as numpy alias for plot reuse
    series = values_np

    # ── Layout ────────────────────────────────────────────────────────────────
    sns.set_theme(style="darkgrid", font_scale=1.05)

    fig = plt.figure(figsize=figsize, facecolor="#0f1117")
    fig.suptitle(
        f"Distribution Analysis  ·  {column}", fontsize=18, fontweight="bold", color="white", y=0.98
    )

    gs = GridSpec(
        2,
        3,
        figure=fig,
        hspace=0.42,
        wspace=0.35,
        left=0.07,
        right=0.97,
        top=0.91,
        bottom=0.08,
    )

    ax_ts = fig.add_subplot(gs[0, :])  # full-width time series
    ax_hist = fig.add_subplot(gs[1, 0])  # histogram + KDE
    ax_box = fig.add_subplot(gs[1, 1])  # box plot
    ax_qq = fig.add_subplot(gs[1, 2])  # Q-Q plot

    panel_bg = "#1a1d27"
    for ax in [ax_ts, ax_hist, ax_box, ax_qq]:
        ax.set_facecolor(panel_bg)
        ax.tick_params(colors="#aaaaaa", labelsize=9)
        for spine in ax.spines.values():
            spine.set_edgecolor("#333344")

    # ── Color scheme ──────────────────────────────────────────────────────────
    C_MAIN = "#4fc3f7"  # cool blue — normal data
    C_OUTLIER = "#ff5370"  # red — outliers
    C_MEAN = "#ffd740"  # amber — mean
    C_MEDIAN = "#69f0ae"  # green — median
    C_FENCE = "#b39ddb"  # purple — IQR fences

    # ══════════════════════════════════════════════════════════════════════════
    # 1 · TIME SERIES  (top row)
    # ══════════════════════════════════════════════════════════════════════════
    # Polars: no index — build x-axis as numpy, filter outlier positions by mask
    null_mask = df[column].is_not_null().to_numpy()  # align x to non-null rows

    if time_col and time_col in df.columns:
        x_all = df[time_col].to_numpy()
    else:
        x_all = np.arange(len(df))

    x_valid = x_all[null_mask]  # same length as series

    ax_ts.plot(
        x_valid, series, color=C_MAIN, linewidth=0.8, alpha=0.85, label=column, zorder=2
    )

    # shade outlier zones
    ax_ts.axhspan(series.min() - std, lower, color=C_OUTLIER, alpha=0.07, zorder=1)
    ax_ts.axhspan(upper, series.max() + std, color=C_OUTLIER, alpha=0.07, zorder=1)

    # fence lines
    ax_ts.axhline(
        upper,
        color=C_FENCE,
        linewidth=1.1,
        linestyle="--",
        alpha=0.75,
        label=f"IQR fence (×{iqr_multiplier})",
    )
    ax_ts.axhline(lower, color=C_FENCE, linewidth=1.1, linestyle="--", alpha=0.75)

    # scatter outliers — use mask_iqr (already aligned to x_valid)
    if mask_iqr.any():
        ax_ts.scatter(
            x_valid[mask_iqr],
            series[mask_iqr],
            color=C_OUTLIER,
            s=30,
            zorder=5,
            label=f"Outliers ({len(outliers_iqr)})",
            edgecolors="white",
            linewidths=0.4,
        )

    ax_ts.axhline(mean, color=C_MEAN, linewidth=1, linestyle=":", alpha=0.9)
    ax_ts.axhline(median, color=C_MEDIAN, linewidth=1, linestyle=":", alpha=0.9)

    ax_ts.set_title("Time Series with Outlier Overlay", color="white", fontsize=11, pad=6)
    ax_ts.set_ylabel(column, color="#aaaaaa", fontsize=9)
    if time_col:
        ax_ts.set_xlabel(time_col, color="#aaaaaa", fontsize=9)

    ax_ts.legend(
        facecolor="#22253a",
        edgecolor="#444466",
        labelcolor="white",
        fontsize=8,
        loc="upper left",
        ncol=3,
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 2 · HISTOGRAM + KDE
    # ══════════════════════════════════════════════════════════════════════════
    sns.histplot(
        series,
        ax=ax_hist,
        kde=True,
        color=C_MAIN,
        alpha=0.55,
        line_kws={"linewidth": 2, "color": C_MAIN},
        edgecolor="#0f1117",
        linewidth=0.4,
        bins="auto",
    )

    # vertical stat lines
    ax_hist.axvline(mean, color=C_MEAN, linestyle="--", linewidth=1.4, label=f"Mean {mean:.2f}")
    ax_hist.axvline(
        median, color=C_MEDIAN, linestyle="--", linewidth=1.4, label=f"Median {median:.2f}"
    )
    ax_hist.axvline(
        lower, color=C_FENCE, linestyle=":", linewidth=1.2, label=f"Fences ±{iqr_multiplier}×IQR"
    )
    ax_hist.axvline(upper, color=C_FENCE, linestyle=":", linewidth=1.2)

    # shade outlier tails
    ax_hist.axvspan(
        series.min() - std, lower, color=C_OUTLIER, alpha=0.12, label="Outlier tail"
    )
    ax_hist.axvspan(upper, series.max() + std, color=C_OUTLIER, alpha=0.12)

    ax_hist.set_title("Histogram + KDE", color="white", fontsize=10, pad=6)
    ax_hist.set_xlabel(column, color="#aaaaaa", fontsize=9)
    ax_hist.set_ylabel("Count", color="#aaaaaa", fontsize=9)
    ax_hist.legend(facecolor="#22253a", edgecolor="#444466", labelcolor="white", fontsize=7.5)

    # annotation box
    textstr = (
        f"Skew   {skewness:+.3f}\n" f"Kurt   {kurtosis:+.3f}\n" f"Outliers  {outlier_pct:.1f}%"
    )
    ax_hist.text(
        0.97,
        0.97,
        textstr,
        transform=ax_hist.transAxes,
        fontsize=8,
        verticalalignment="top",
        horizontalalignment="right",
        color="white",
        bbox=dict(
            boxstyle="round,pad=0.4", facecolor="#22253a", edgecolor="#555577", alpha=0.85
        ),
    )

    # ══════════════════════════════════════════════════════════════════════════
    # 3 · BOX PLOT
    # ══════════════════════════════════════════════════════════════════════════
    bp = ax_box.boxplot(
        series,
        patch_artist=True,
        notch=True,
        vert=True,
        widths=0.45,
        showfliers=True,
        flierprops=dict(marker="o", color=C_OUTLIER, alpha=0.6, markersize=4, markeredgewidth=0),
        medianprops=dict(color=C_MEDIAN, linewidth=2),
        whiskerprops=dict(color=C_FENCE, linewidth=1.2, linestyle="--"),
        capprops=dict(color=C_FENCE, linewidth=1.5),
        boxprops=dict(linewidth=1.2),
    )
    bp["boxes"][0].set_facecolor("#4fc3f720")

    # violin overlay
    vp = ax_box.violinplot(
        series, positions=[1], widths=0.7, showmeans=False, showmedians=False, showextrema=False
    )
    for body in vp["bodies"]:
        body.set_facecolor(C_MAIN)
        body.set_alpha(0.18)
        body.set_edgecolor("none")

    ax_box.set_title("Box + Violin Plot", color="white", fontsize=10, pad=6)
    ax_box.set_xticks([])
    ax_box.set_ylabel(column, color="#aaaaaa", fontsize=9)

    # annotate IQR fences
    for val, label in [
        (lower, f"Lower fence\n{lower:.2f}"),
        (upper, f"Upper fence\n{upper:.2f}"),
    ]:
        ax_box.annotate(
            label,
            xy=(1, val),
            xytext=(1.35, val),
            color=C_FENCE,
            fontsize=7.5,
            arrowprops=dict(arrowstyle="-", color=C_FENCE, lw=0.8),
            va="center",
        )

    # ══════════════════════════════════════════════════════════════════════════
    # 4 · Q-Q PLOT
    # ══════════════════════════════════════════════════════════════════════════
    (osm, osr), (slope, intercept, r) = stats.probplot(series, dist="norm")

    # color points by z-score distance
    z_vals = np.abs((np.sort(series) - mean) / std)
    scatter = ax_qq.scatter(
        osm, osr, c=z_vals, cmap="coolwarm", s=12, alpha=0.7, vmin=0, vmax=4, zorder=3
    )
    # reference line
    line_x = np.array([osm.min(), osm.max()])
    ax_qq.plot(
        line_x,
        slope * line_x + intercept,
        color=C_MEAN,
        linewidth=1.5,
        linestyle="--",
        label=f"R²={r**2:.4f}",
        zorder=2,
    )

    cb = plt.colorbar(scatter, ax=ax_qq, pad=0.02)
    cb.set_label("|z-score|", color="white", fontsize=8)
    cb.ax.yaxis.set_tick_params(color="white", labelcolor="white", labelsize=7)
    cb.outline.set_edgecolor("#333344")

    ax_qq.set_title("Q-Q Plot  (Normality + Extremes)", color="white", fontsize=10, pad=6)
    ax_qq.set_xlabel("Theoretical quantiles", color="#aaaaaa", fontsize=9)
    ax_qq.set_ylabel("Sample quantiles", color="#aaaaaa", fontsize=9)
    ax_qq.legend(facecolor="#22253a", edgecolor="#444466", labelcolor="white", fontsize=8)

    # ── Global stat ribbon ────────────────────────────────────────────────────
    stat_line = (
        f"n={len(series):,}    mean={mean:.3f}    median={median:.3f}    "
        f"std={std:.3f}    IQR={iqr:.3f}    "
        f"[{lower:.3f} , {upper:.3f}]  fences    "
        f"IQR-outliers={len(outliers_iqr)}    Z>3-outliers={len(outliers_z)}"
    )
    fig.text(
        0.5,
        0.005,
        stat_line,
        ha="center",
        va="bottom",
        fontsize=8,
        color="#888899",
        fontfamily="monospace",
    )

    save_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(save_path, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)