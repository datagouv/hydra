"""Common type definitions"""

from typing import Literal

FileFormatLiteral = Literal[
    "csv", "gz", "xls", "xlsx", "geojson", "parquet", "wfs", "wms", "unknown"
]

OgcFormatLiteral = Literal["wfs", "wms"]
