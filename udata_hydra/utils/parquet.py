from io import BytesIO

import pandas as pd


def save_as_parquet(
    df: pd.DataFrame,
    output_filename: str | None = None,
) -> tuple[str, BytesIO | None]:
    bytes = df.to_parquet(
        f"{output_filename}.parquet" if output_filename else None,
        compression="zstd",  # best compression to date
    )
    return f"{output_filename}.parquet", bytes
