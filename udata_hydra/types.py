"""Common type definitions"""

from typing import Literal

FileFormatLiteral = Literal[
    "csv", "csvgz", "xls", "xlsx", "geojson", "parquet", "wfs", "wms", "unknown"
]

OgcFormatLiteral = Literal["wfs", "wms"]
