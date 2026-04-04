"""
Statsmodels Display Configuration

This module sets up statsmodels display settings for better readability
when working with statistical models in notebooks.
"""

import statsmodels.api as sm
from statsmodels.iolib.table import SimpleTable


def configure_statsmodels() -> None:
    """
    Configure statsmodels display settings for better model output visualization.

    Settings applied:
    - float_format: 8 decimal places for coefficients and p-values
    - line_width: No limit for summary tables
    """
    SimpleTable.default_float_format = '%.8f'
    SimpleTable.default_width = None


# Apply configuration on import
configure_statsmodels()