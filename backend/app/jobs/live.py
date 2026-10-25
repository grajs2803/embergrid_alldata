"""Live data sources: NASA FIRMS (hotspots) and Open-Meteo (weather). Pure fetch + parse; nothing here touches the DB.
All network access goes through _get(), so tests replace that one function.

Environment: FIRMS_MAP_KEY (required for fire), FIRMS_SOURCE (default VIIRS_NOAA20_NRT)
"""
import io
import logging
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests

log = logging.getLogger("embergrid.live")

FIRMS_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{key}/{source}/{west},{south},{east},{north}/{days}/{date}"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
# the exact variable names and model used by your historical downloads (weather_manifest.json)
DAILY_VARS = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum",
              "relative_humidity_2m_mean", "wind_speed_10m_mean", "wind_speed_10m_max", "wind_direction_10m_dominant"]


def _get(url, params=None, retries=4, timeout=90):
    """GET with exponential backoff on 429/5xx and network errors."""
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                last = RuntimeError(f"HTTP {r.status_code} from {url.split('?')[0]}")
            else:
                r.raise_for_status()
                return r
        except requests.RequestException as exc:
            last = exc
        time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"giving up on {url.split('?')[0]} after {retries} attempts: {last}")


def fetch_firms(map_key, source, bbox, start: date, end: date):
    """bbox = (west, south, east, north). FIRMS allows at most 5 days per call, so the range is chunked."""
    west, south, east, north = bbox
    frames, d = [], start
    while d <= end:
        days = min(5, (end - d).days + 1)
        r = _get(FIRMS_URL.format(key=map_key, source=source, west=round(west, 4), south=round(south, 4),
                                  east=round(east, 4), north=round(north, 4), days=days, date=d.isoformat()))
        text = r.text.strip()
        if text and not text.startswith("latitude"):
            # an invalid key / quota message is plain text, not a CSV: fail loudly instead of ingesting nothing
            raise RuntimeError(f"FIRMS returned an unexpected response: {text[:200]!r}")
        if text:
            frames.append(pd.read_csv(io.StringIO(text), dtype={"acq_time": str, "confidence": str}))
        d += timedelta(days=days)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_openmeteo(points: pd.DataFrame, start: date, end: date, kind: str, wind_unit="ms", today=None, batch=25):
    """points: weather_point_id, lat, lon. kind: 'archive' (ERA5, final, ~5 day lag) or 'forecast' (provisional).
    Returns one row per point per day with the same column names as your historical CSVs."""
    out = []
    for i in range(0, len(points), batch):
        chunk = points.iloc[i:i + batch]
        params = {"latitude": ",".join(f"{v:.4f}" for v in chunk.lat), "longitude": ",".join(f"{v:.4f}" for v in chunk.lon),
                  "daily": ",".join(DAILY_VARS), "timezone": "Asia/Kolkata", "wind_speed_unit": wind_unit}
        if kind == "archive":
            url = ARCHIVE_URL
            params.update(start_date=start.isoformat(), end_date=end.isoformat(), models="era5")
        else:
            url = FORECAST_URL
            t = today or date.today()
            params.update(past_days=max((t - start).days, 0), forecast_days=max((end - t).days + 1, 1))
        data = _get(url, params=params).json()
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"Open-Meteo error: {data.get('reason')}")
        items = data if isinstance(data, list) else [data]
        if len(items) != len(chunk):
            raise RuntimeError(f"Open-Meteo returned {len(items)} locations for {len(chunk)} requested")
        for pid, item in zip(chunk.weather_point_id, items):
            daily = item["daily"]
            df = pd.DataFrame({"weather_point_id": pid, "date": pd.to_datetime(daily["time"]).date})
            for v in DAILY_VARS:
                df[v] = pd.to_numeric(pd.Series(daily[v], dtype="float64"), errors="coerce").values
            out.append(df)
    res = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    if not res.empty:
        res = res[(res.date >= start) & (res.date <= end)]
    return res