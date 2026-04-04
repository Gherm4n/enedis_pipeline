"""
Polars Display Configuration

This module sets up polars display settings for better readability
when working with DataFrames in notebooks.
"""

import polars as pl


def configure_polars() -> None:
    """
    Configure polars display settings for better DataFrame visualization.

    Settings applied:
    - tbl_cols: -1 (shows all columns)
    - tbl_rows: -1 (shows all rows)
    - tbl_width: -1 (no width limit)
    - fmt_str_lengths: Truncate long strings with ellipsis
    - float_precision: 4 decimal places
    - hide_cols_dtypes: True (hide table column data types)
    """
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(10)
    pl.Config.set_tbl_width_chars(-1)
    pl.Config.set_float_precision(4)
    pl.Config.set_fmt_str_lengths(50)
    pl.Config.set_tbl_hide_column_data_types(True)

# Apply configuration on import
configure_polars()