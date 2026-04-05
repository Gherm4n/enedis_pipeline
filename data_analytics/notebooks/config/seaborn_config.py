"""
Seaborn Display Configuration

This module sets up seaborn display settings for better visualization
when working with statistical plots in notebooks.
"""

import seaborn as sns


def configure_seaborn() -> None:
    """
    Configure seaborn display settings for better plot visualization.

    Settings applied:
    - context: notebook (balanced element sizes)
    - palette: muted (soft colors)
    - font_scale: 1.2 (slightly larger fonts)
    - style: whitegrid
    """
    # Set the aesthetic style
    sns.set_style('whitegrid')

    # Set context for plot element sizes
    # Options: paper, notebook, talk, poster
    sns.set_context('notebook')

    # Set color palette
    # Options: deep, muted, pastel, bright, dark, colorblind
    sns.set_palette('muted')

    # Set font scale multiplier
    sns.set_context('notebook', font_scale=1.2)


# Apply configuration on import
configure_seaborn()