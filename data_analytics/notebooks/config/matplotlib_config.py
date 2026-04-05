"""
Matplotlib Display Configuration

This module sets up matplotlib display settings for better visualization
when working with plots in notebooks.
"""

import matplotlib.pyplot as plt
import matplotlib as mpl


def configure_matplotlib() -> None:
    """
    Configure matplotlib display settings for better plot visualization.

    Settings applied:
    - figure.figsize: 12x8 inches
    - figure.dpi: 150
    - font.size: 12pt
    - style: whitegrid
    - axes.grid: True
    - lines.linewidth: 1.5
    - savefig.dpi: 150
    - Backend: notebook (interactive)
    """
    # Set the style
    plt.style.use('seaborn-v0_8-whitegrid')

    # Figure size (width, height in inches)
    mpl.rcParams['figure.figsize'] = (12, 8)

    # Display resolution
    mpl.rcParams['figure.dpi'] = 150

    # Font size for labels and titles
    mpl.rcParams['font.size'] = 12

    # Enable grid by default
    mpl.rcParams['axes.grid'] = True

    # Line width for plot lines
    mpl.rcParams['lines.linewidth'] = 1.5

    # Save figure resolution
    mpl.rcParams['savefig.dpi'] = 150

    # Tight layout to prevent label cutoff
    mpl.rcParams['figure.autolayout'] = True


# Apply configuration on import
configure_matplotlib()