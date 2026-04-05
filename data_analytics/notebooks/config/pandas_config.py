"""
Pandas Display Configuration

This module sets up pandas display settings for better readability
when working with DataFrames in notebooks.
"""

import pandas as pd


def configure_pandas() -> None:
    """
    Configure pandas display settings for better DataFrame visualization.

    Settings applied:
    - display.max_columns: None (shows all columns)
    - display.max_rows: 20 (compact row display)
    - display.float_format: 2 decimal places
    - display.max_colwidth: None (shows full column content)
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 20)
    pd.set_option('display.float_format', '{:.2f}'.format)
    pd.set_option('display.max_colwidth', None)


# Apply configuration on import
configure_pandas()