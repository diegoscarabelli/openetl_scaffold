"""
WID pipeline constants and configuration.

Defines the World Inequality Database API configuration, variable and percentile
definitions, and file type patterns for the WID pipeline.
"""

import os
import re
from enum import Enum

# -------------------------------------------------------------------
# WID API Configuration
# -------------------------------------------------------------------

WID_API_BASE_URL = "https://rfap9nitz6.execute-api.eu-west-1.amazonaws.com/prod/"

# Shared API key from the open-source WID R package (wid-r-tool).
# Not user-specific. Override via WID_API_KEY env var if needed.
WID_API_KEY = os.environ.get(
    "WID_API_KEY",
    "rYFByOB0ioaPATwHtllMI71zLOZSK0Ic5veQonJP",
)

# WID demographic parameters.
WID_AGE_CODE = "992"  # Adults 20+.
WID_POP_CODE = "j"  # Equal-split adults.

# API batching parameters.
COUNTRY_BATCH_SIZE = 20


# -------------------------------------------------------------------
# File Types
# -------------------------------------------------------------------


class WIDFileTypes(Enum):
    """
    File type patterns for WID pipeline data files.
    """

    COUNTRY = re.compile(r"countries_.*\.json$")
    VARIABLE_META = re.compile(r"variable_metadata_.*\.json$")
    OBSERVATION = re.compile(r"observations_.*\.json$")


# -------------------------------------------------------------------
# Variables
# -------------------------------------------------------------------

# WID sixlet codes tracked by this pipeline.
# Keys are the sixlet (series_type + concept), values hold
# component parts and fallback descriptions used when the
# metadata API call fails.
VARIABLES = {
    "sptinc": {
        "concept": "ptinc",
        "series_type": "s",
        "fallback_name": "Pre-tax national income share",
        "fallback_unit": "share",
    },
    "shweal": {
        "concept": "hweal",
        "series_type": "s",
        "fallback_name": "Net personal wealth share",
        "fallback_unit": "share",
    },
}

# -------------------------------------------------------------------
# Percentiles
# -------------------------------------------------------------------

# Each tuple: (code, lower_bound, upper_bound, width, granularity).
PERCENTILES = [
    # Deciles (10% slices across 0-100%).
    ("p0p10", 0, 10, 10, "decile"),
    ("p10p20", 10, 20, 10, "decile"),
    ("p20p30", 20, 30, 10, "decile"),
    ("p30p40", 30, 40, 10, "decile"),
    ("p40p50", 40, 50, 10, "decile"),
    ("p50p60", 50, 60, 10, "decile"),
    ("p60p70", 60, 70, 10, "decile"),
    ("p70p80", 70, 80, 10, "decile"),
    ("p80p90", 80, 90, 10, "decile"),
    ("p90p100", 90, 100, 10, "decile"),
    # Percentiles (1% slices within top 10%).
    ("p90p91", 90, 91, 1, "percentile"),
    ("p91p92", 91, 92, 1, "percentile"),
    ("p92p93", 92, 93, 1, "percentile"),
    ("p93p94", 93, 94, 1, "percentile"),
    ("p94p95", 94, 95, 1, "percentile"),
    ("p95p96", 95, 96, 1, "percentile"),
    ("p96p97", 96, 97, 1, "percentile"),
    ("p97p98", 97, 98, 1, "percentile"),
    ("p98p99", 98, 99, 1, "percentile"),
    ("p99p100", 99, 100, 1, "percentile"),
    # Milles (0.1% slices within top 1%).
    ("p99p99.1", 99, 99.1, 0.1, "mille"),
    ("p99.1p99.2", 99.1, 99.2, 0.1, "mille"),
    ("p99.2p99.3", 99.2, 99.3, 0.1, "mille"),
    ("p99.3p99.4", 99.3, 99.4, 0.1, "mille"),
    ("p99.4p99.5", 99.4, 99.5, 0.1, "mille"),
    ("p99.5p99.6", 99.5, 99.6, 0.1, "mille"),
    ("p99.6p99.7", 99.6, 99.7, 0.1, "mille"),
    ("p99.7p99.8", 99.7, 99.8, 0.1, "mille"),
    ("p99.8p99.9", 99.8, 99.9, 0.1, "mille"),
    ("p99.9p100", 99.9, 100, 0.1, "mille"),
    # Basis points (0.01% slices within top 0.1%).
    ("p99.9p99.91", 99.9, 99.91, 0.01, "basis_point"),
    ("p99.91p99.92", 99.91, 99.92, 0.01, "basis_point"),
    ("p99.92p99.93", 99.92, 99.93, 0.01, "basis_point"),
    ("p99.93p99.94", 99.93, 99.94, 0.01, "basis_point"),
    ("p99.94p99.95", 99.94, 99.95, 0.01, "basis_point"),
    ("p99.95p99.96", 99.95, 99.96, 0.01, "basis_point"),
    ("p99.96p99.97", 99.96, 99.97, 0.01, "basis_point"),
    ("p99.97p99.98", 99.97, 99.98, 0.01, "basis_point"),
    ("p99.98p99.99", 99.98, 99.99, 0.01, "basis_point"),
    ("p99.99p100", 99.99, 100, 0.01, "basis_point"),
]
