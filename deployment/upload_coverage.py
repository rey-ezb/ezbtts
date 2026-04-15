from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class UploadCoverage:
    start_date: str | None
    end_date: str | None


def normalize_column_name(value: str) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in str(value)).split())


def pick_date_column(columns: list[str], upload_type: str) -> str | None:
    normalized = {column: normalize_column_name(column) for column in columns}
    candidates: list[tuple[str, ...]]
    if upload_type == "statements":
        candidates = [
            ("statement", "date"),
            ("date",),
        ]
    else:
        candidates = [
            ("paid", "time"),
            ("order", "date"),
            ("paid", "date"),
            ("created", "time"),
            ("created", "date"),
        ]
    for keywords in candidates:
        for column, normalized_name in normalized.items():
            if all(keyword in normalized_name for keyword in keywords):
                return column
    return None


def infer_month_coverage_from_filename(filename: str) -> UploadCoverage:
    month_lookup = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    text = Path(filename).stem.lower()
    for month_name, month_number in month_lookup.items():
        if month_name not in text:
            continue
        for token in text.split():
            if token.isdigit() and len(token) == 4:
                year = int(token)
                start = pd.Timestamp(year=year, month=month_number, day=1)
                end = start + pd.offsets.MonthEnd(0)
                return UploadCoverage(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    return UploadCoverage(None, None)


def read_upload_dataframe(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)
    return pd.DataFrame()


def infer_upload_coverage(file_path: Path, upload_type: str) -> UploadCoverage:
    try:
        frame = read_upload_dataframe(file_path)
    except Exception:  # noqa: BLE001
        frame = pd.DataFrame()
    if not frame.empty:
        date_column = pick_date_column(list(frame.columns), upload_type)
        if date_column:
            dates = pd.to_datetime(frame[date_column], errors="coerce").dropna()
            if not dates.empty:
                return UploadCoverage(dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d"))
    return infer_month_coverage_from_filename(file_path.name)


def date_to_ordinal(value: str | None) -> int | None:
    if not value:
        return None
    return pd.Timestamp(value).toordinal()


def date_range_fully_covered(start: str | None, end: str | None, newer_ranges: list[tuple[str | None, str | None]]) -> bool:
    start_ord = date_to_ordinal(start)
    end_ord = date_to_ordinal(end)
    if start_ord is None or end_ord is None:
        return False
    merged: list[list[int]] = []
    for newer_start, newer_end in newer_ranges:
        newer_start_ord = date_to_ordinal(newer_start)
        newer_end_ord = date_to_ordinal(newer_end)
        if newer_start_ord is None or newer_end_ord is None:
            continue
        merged.append([newer_start_ord, newer_end_ord])
    if not merged:
        return False
    merged.sort(key=lambda item: item[0])
    collapsed: list[list[int]] = [merged[0]]
    for current_start, current_end in merged[1:]:
        tail = collapsed[-1]
        if current_start <= tail[1] + 1:
            tail[1] = max(tail[1], current_end)
        else:
            collapsed.append([current_start, current_end])
    position = start_ord
    for window_start, window_end in collapsed:
        if position < window_start:
            return False
        if position <= window_end:
            position = window_end + 1
        if position > end_ord:
            return True
    return position > end_ord


def determine_active_uploads(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("upload_type") or "").strip().lower(), []).append(row)

    resolved: list[dict[str, Any]] = []
    for upload_type, items in grouped.items():
        ordered = sorted(items, key=lambda item: (item.get("uploaded_at") or "", item.get("id") or ""), reverse=True)
        newer_ranges: list[tuple[str | None, str | None]] = []
        for item in ordered:
            start = item.get("start_date")
            end = item.get("end_date")
            superseded = date_range_fully_covered(start, end, newer_ranges)
            resolved.append(
                {
                    **item,
                    "upload_type": upload_type,
                    "is_active": not superseded,
                    "replaced_by_batch_id": (ordered[0].get("upload_batch_id") or ordered[0].get("id")) if superseded and ordered else None,
                }
            )
            if start and end:
                newer_ranges.append((start, end))
    return resolved
