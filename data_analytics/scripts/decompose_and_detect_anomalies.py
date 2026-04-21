import polars as pl
import polars.selectors as cs
from pathlib import Path
import sys
import numpy as np
from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt
import seaborn as sns

# Add project root to sys.path
PROJ_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJ_ROOT))

from src.config import FIGURES_DIR, PROCESSED_DATA_DIR
from utils.plots import plot_decomposition

def detect_anomalies_from_decomp(original, trend, seasonal, threshold=3.0):
    """
    Detects anomalies based on the residuals of seasonal decomposition.
    """
    # normal = trend + seasonal
    # residual = original - normal
    # But seasonal_decompose returns resid = original - trend - seasonal
    residual = original - trend - seasonal
    
    # Handle NaNs (at the beginning/end of trend)
    mask = ~np.isnan(residual)
    if not np.any(mask):
        return np.zeros_like(original, dtype=bool)
    
    res_clean = residual[mask]
    mean = np.mean(res_clean)
    std = np.std(res_clean)
    
    anomalies = np.abs(residual - mean) > threshold * std
    return anomalies.filled(False) if hasattr(anomalies, 'filled') else np.nan_to_num(anomalies, nan=False).astype(bool)

def main():
    # Find all relevant parquet files
    all_files = list(PROCESSED_DATA_DIR.glob("enedis_dataset_engineered_feature_*.parquet"))
    all_files += list(PROCESSED_DATA_DIR.glob("enedis_dataset_year_*.parquet"))
    
    # Map year to best file
    year_to_file = {}
    for f in all_files:
        if "with_decomposition_anomalies" in f.name or "with_anomalies" in f.name:
            continue
        
        # Extract year
        import re
        match = re.search(r"202\d", f.name)
        if match:
            year = match.group()
            # If we have an engineered feature file, it takes priority
            if year not in year_to_file or "engineered" in f.name:
                year_to_file[year] = f

    years = sorted(year_to_file.keys())

    for year in years:
        file_path = year_to_file[year]
        print(f"Processing year {year} (from {file_path.name})...")
        
        df = pl.read_parquet(file_path)
        
        # Identify columns
        all_cols = df.columns
        cons_cols = [c for c in all_cols if c.startswith("consommation_") and not c.endswith("_totale")]
        prod_cols = [c for c in all_cols if c.startswith("production_") and not c.endswith("_totale")]
        
        # Exclude other specific ones if they somehow slipped in
        exclude_keywords = ["Price", "Temperature", "degre_jour", "ecart_temperature", "pseudo_rayonnement"]
        
        def filter_cols(cols):
            return [c for c in cols if not any(k in c for k in exclude_keywords)]

        cons_cols = filter_cols(cons_cols)
        prod_cols = filter_cols(prod_cols)
        
        target_cols = cons_cols + prod_cols
        
        # We will store anomalies here
        df_with_anomalies = df.clone()
        
        for col in target_cols:
            print(f"  Decomposing {col}...")
            
            # Sub-directory for the category
            category = "Consommation" if col.startswith("consommation_") else "Production"
            if col.startswith("consommation_"):
                if "hta" in col.lower():
                    category += "_HTA"
                elif "btsup" in col.lower():
                    category += "_BTSUP"
                elif "pro" in col.lower() and "profilee" in col.lower():
                    category += "_PRO"
                elif "res" in col.lower() and "profilee" in col.lower():
                    category += "_RESIDENTIELLE"
                elif "professionnelle" in col.lower():
                    category += "_PRO"
                elif "residentielle" in col.lower():
                    category += "_RESIDENTIELLE"
            else:
                if "photovoltaique" in col.lower():
                    category += "-Photovoltaique"
                elif "eolien" in col.lower():
                    category += "-Eolien"
                elif "cogeneration" in col.lower():
                    category += "-Cogeneration"
                elif "autre" in col.lower():
                    category += "-Autre"
                elif "profilee" in col.lower():
                    # For production_profilee...
                    if "photovoltaique" in col.lower(): category = "Production-Photovoltaique"
                    elif "cogeneration" in col.lower(): category = "Production-Cogeneration"
                    elif "hydraulique" in col.lower(): category = "Production-Hydraulique" # Added just in case
                    else: category = "Production-Profilee"

            save_path = FIGURES_DIR / year / category
            
            # Period 48 (24h)
            plot_decomposition(df, col, 48, save_path, f"Decomposition_{col}_24.0h")
            
            # Period 336 (168h)
            plot_decomposition(df, col, 336, save_path, f"Decomposition_{col}_168.0h")
            
            # Anomaly detection for consommation_telerelevee
            if col.startswith("consommation_telerelevee"):
                print(f"    Detecting anomalies for {col}...")
                series_data = df.select(col).to_numpy().ravel()
                
                # Perform decomposition manually to get values
                # We use 336 for anomaly detection as it's more comprehensive
                if len(series_data) > 336 * 2:
                    decomp = seasonal_decompose(series_data, model="additive", period=336)
                    anomalies = detect_anomalies_from_decomp(series_data, decomp.trend, decomp.seasonal)
                    
                    anomaly_col_name = f"{col}_anomalie_by_decomposition"
                    df_with_anomalies = df_with_anomalies.with_columns(
                        pl.Series(anomaly_col_name, anomalies)
                    )
                    
                    # Plot the result in the base Consommation/Production directory
                    base_category = "Consommation" if col.startswith("consommation_") else "Production"
                    anomaly_save_path = FIGURES_DIR / year / base_category
                    plot_anomalies(df_with_anomalies, col, anomaly_col_name, anomaly_save_path, year)

        # Save the dataset with anomalies
        output_path = PROCESSED_DATA_DIR / f"enedis_dataset_year_{year}_with_decomposition_anomalies.parquet"
        df_with_anomalies.write_parquet(output_path)

def plot_anomalies(df, col, anomaly_col, save_path, year):
    save_file = save_path / f"Anomaly_{col}.png"
    # if save_file.exists():
    #     return

    plt.figure(figsize=(20, 10))
    sns.set_style("darkgrid")
    
    # Plot normal data
    plt.plot(df["timestamp"], df[col], color='blue', alpha=0.5, label='Consommation')
    
    # Plot anomalies
    anomalies = df.filter(pl.col(anomaly_col))
    if anomalies.height > 0:
        plt.scatter(anomalies["timestamp"], anomalies[col], color='red', s=20, label='Anomalie')
    
    plt.title(f"Anomalies détectées par décomposition — {col} ({year})")
    plt.xlabel("Timestamp")
    plt.ylabel("Valeur")
    plt.legend()
    
    save_path.mkdir(parents=True, exist_ok=True)
    plt.savefig(save_file, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    main()
