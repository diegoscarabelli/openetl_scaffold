"""
File type definitions for the example pipeline.

Copy to your pipeline directory and replace ExampleFileTypes. Each member value is a
compiled regex matched against filenames in ingest/.
"""

import re
from enum import Enum


class ExampleFileTypes(Enum):
    # Define one member per distinct file type your pipeline ingests.
    DATA = re.compile(r".*\.csv$")
