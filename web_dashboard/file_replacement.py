from __future__ import annotations

from typing import Sequence

import pandas as pd


def keep_latest_file_rows_by_date(
    df: pd.DataFrame,
    *,
    date_column: str,
    file_column: str,
    mtime_column: str,
    partition_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    if date_column not in df.columns or file_column not in df.columns or mtime_column not in df.columns:
        return df.copy()

    partition_columns = list(partition_columns or [])
    working = df.dropna(subset=[date_column]).copy()
    if working.empty:
        return working

    version_columns = [*partition_columns, date_column, file_column, mtime_column]
    versions = working[version_columns].drop_duplicates()
    versions = versions.sort_values(
        [*partition_columns, date_column, mtime_column, file_column],
        ascending=[*([True] * len(partition_columns)), True, True, True],
    )
    latest = versions.groupby([*partition_columns, date_column], as_index=False).tail(1)
    latest = latest[[*partition_columns, date_column, file_column]].rename(columns={file_column: "__latest_file"})
    merged = working.merge(latest, on=[*partition_columns, date_column], how="inner")
    filtered = merged.loc[merged[file_column] == merged["__latest_file"]].drop(columns=["__latest_file"])
    return filtered.reset_index(drop=True)
