import polars as pl
from statsmodels.tsa.stattools import adfuller

def test_stationarity(df: pl.DataFrame, columns: list, lag: int = None):
    """
    Performs the Augmented Dickey-Fuller test on specified columns of a Polars DataFrame.
    """
    for column in columns:
        if column not in df.columns:
            continue
            
        # Drop nulls for the test
        series_data = df.select(column).drop_nulls().to_numpy().ravel()
        
        if len(series_data) == 0:
            print(f"Column {column} has no data after dropping nulls. Skipping.")
            continue

        res = adfuller(series_data, maxlag=lag, autolag=None if lag else 'AIC')
        
        print(f"{'-'*20} Column: {column} {'-'*20}")
        print(f"Augmented Dickey-Fuller Statistic: {res[0]:.4f}")
        print(f"p-value: {res[1]:.4f}")
        print(f"Lags used: {res[2]}")
        print(f"Number of observations: {res[3]}")
        print('Critical values at different levels:')
        for k, v in res[4].items():
            print(f"  {k}: {v:.3f}")
        
        is_stationary = res[1] < 0.05
        print(f"Stationary (p < 0.05): {is_stationary}")
        print("-" * 80, "\n")
