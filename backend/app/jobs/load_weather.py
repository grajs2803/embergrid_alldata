"""Load the pre-downloaded Open-Meteo (ERA5 daily) files into Postgres. Every step is idempotent and logged.

  python -m app.jobs.load_weather points   <weather_points.csv>
  python -m app.jobs.load_weather cellmap  <cell_weather_map.csv>
  python -m app.jobs.load_weather daily    "<dir>/era5_daily_*_batch_*.csv"
  python -m app.jobs.load_weather fwi      [--wind-unit ms|kmh]
  python -m app.jobs.load_weather features 2020-02-01 2020-04-30      # calls the SQL procedure

Order: points -> cellmap -> daily -> fwi -> (label layer exists) -> features.
"""
import argparse
import glob
import io
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

from app import create_app
from app.extensions import db
from app.jobs import fwi as fwi_lib
from app.jobs.db_util import call_procedure
from app.models.ingestion_run import IngestionRun

STEP = 0.01
RAW = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum",
       "relative_humidity_2m_mean", "wind_speed_10m_mean", "wind_speed_10m_max", "wind_direction_10m_dominant"]


def global_cell_id(lat, lon):
    """Same rule as the cells table: anchored at (0,0); round() first so 479.99999 does not floor to 479."""
    r = np.floor(np.round(np.asarray(lat) / STEP, 9)).astype(int)
    c = np.floor(np.round(np.asarray(lon) / STEP, 9)).astype(int)
    return np.array([f"r{a:04d}c{b:04d}" for a, b in zip(r, c)])


def _start(source, job_type, **kw):
    # ingestion_runs.source is varchar(40) in the real DB: log the file NAME (or glob), never the full path
    run = IngestionRun(source=source.replace("\\", "/").split("/")[-1][:40], job_type=job_type, status="running", **kw)
    db.session.add(run)
    db.session.commit()
    return run


def _finish(run, **counts):
    for k, v in counts.items():
        setattr(run, k, v)
    run.status, run.finished_at = "success", datetime.now(timezone.utc)
    db.session.commit()


def _fail(run, exc):
    db.session.rollback()
    run.status, run.finished_at, run.error_message = "failed", datetime.now(timezone.utc), str(exc)[:500]
    db.session.commit()
    raise exc


def _copy_to_temp(cur, df, name, ddl):
    cur.execute(f"CREATE TEMP TABLE {name} ({ddl}) ON COMMIT DROP")
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur.copy_expert(f"COPY {name} FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def load_points(path):
    run = _start("weather_points.csv", "weather_points")
    try:
        df = pd.read_csv(path)[["weather_point_id", "sample_lat", "sample_lon"]]
        conn = db.engine.raw_connection()
        try:
            cur = conn.cursor()
            _copy_to_temp(cur, df, "tmp_pts", "id varchar, lat float8, lon float8")
            cur.execute("""INSERT INTO weather_points (weather_point_id, lat, lon)
                           SELECT id, lat, lon FROM tmp_pts
                           ON CONFLICT (weather_point_id) DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon""")
            conn.commit()
        finally:
            conn.close()
        _finish(run, records_read=len(df), records_inserted=len(df))
    except Exception as exc:
        _fail(run, exc)


def load_cellmap(path):
    """The CSV ids are local to the download box (BST_r0000_c0000). Rebuild the GLOBAL id from the cell centre."""
    run = _start("cell_weather_map.csv", "cell_weather_map")
    try:
        df = pd.read_csv(path)
        df["cell_id"] = global_cell_id(df.cell_lat.values, df.cell_lon.values)
        df = df[["cell_id", "weather_point_id"]].drop_duplicates("cell_id")
        conn = db.engine.raw_connection()
        try:
            cur = conn.cursor()
            _copy_to_temp(cur, df, "tmp_map", "cell_id varchar, wp varchar")
            cur.execute("""INSERT INTO cell_weather_map (cell_id, weather_point_id)
                           SELECT t.cell_id, t.wp FROM tmp_map t JOIN cells c USING (cell_id)
                           ON CONFLICT (cell_id) DO UPDATE SET weather_point_id = EXCLUDED.weather_point_id""")
            ok = cur.rowcount
            conn.commit()
        finally:
            conn.close()
        _finish(run, records_read=len(df), records_inserted=ok, records_rejected=len(df) - ok)
        print(f"mapped {ok:,} of {len(df):,} cells ({len(df) - ok:,} not in cells table)")
    except Exception as exc:
        _fail(run, exc)


def load_daily(pattern):
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"no files match {pattern}")
    run = _start(pattern, "weather_daily")
    total = 0
    try:
        conn = db.engine.raw_connection()
        try:
            for f in files:
                df = pd.read_csv(f)
                cols = ["weather_point_id", "date"] + RAW
                cur = conn.cursor()
                _copy_to_temp(cur, df[cols], "tmp_w",
                              "id varchar, date date, " + ", ".join(f"{c} float8" for c in RAW))
                cur.execute(f"""INSERT INTO weather_daily (weather_point_id, date, {', '.join(RAW)})
                                SELECT * FROM tmp_w
                                ON CONFLICT (weather_point_id, date) DO UPDATE SET
                                {', '.join(f'{c} = EXCLUDED.{c}' for c in RAW)}""")
                if "returned_elevation_m" in df:
                    el = df.groupby("weather_point_id").returned_elevation_m.first().reset_index()
                    _copy_to_temp(cur, el, "tmp_el", "id varchar, el float8")
                    cur.execute("UPDATE weather_points p SET elevation_m = t.el FROM tmp_el t WHERE p.weather_point_id = t.id")
                conn.commit()
                total += len(df)
        finally:
            conn.close()
        _finish(run, records_read=total, records_inserted=total)
        print(f"loaded {total:,} point-days from {len(files)} files")
    except Exception as exc:
        _fail(run, exc)


def upsert_weather_df(df, method):
    """Upsert live/reconciliation rows (columns as the historical CSVs). method: 'era5' or 'forecast'.
    Returns the number of rows written."""
    if df.empty:
        return 0
    df = df[["weather_point_id", "date"] + RAW].copy()
    df["method"] = method
    conn = db.engine.raw_connection()
    try:
        cur = conn.cursor()
        _copy_to_temp(cur, df, "tmp_wl", "id varchar, date date, " + ", ".join(f"{c} float8" for c in RAW) + ", method varchar")
        cur.execute(f"""INSERT INTO weather_daily (weather_point_id, date, {', '.join(RAW)}, method)
                        SELECT * FROM tmp_wl
                        ON CONFLICT (weather_point_id, date) DO UPDATE SET
                        {', '.join(f'{c} = EXCLUDED.{c}' for c in RAW)}, method = EXCLUDED.method""")
        conn.commit()
    finally:
        conn.close()
    return len(df)


def compute_fwi(wind_unit="ms", batch_points=200, point_ids=None):
    """FWI is recursive in time, so it is computed per weather point in date order (restart after gaps)."""
    run = _start("weather_daily", "fwi")
    done = 0
    try:
        ids = [r[0] for r in db.session.execute(db.text("SELECT weather_point_id FROM weather_points ORDER BY 1"))]
        if point_ids is not None:
            ids = [i for i in ids if i in set(point_ids)]
        for i in range(0, len(ids), batch_points):
            chunk = ids[i:i + batch_points]
            df = pd.read_sql(db.text("""SELECT weather_point_id, date, temperature_2m_max tmax, temperature_2m_min tmin,
                                        relative_humidity_2m_mean rh_mean, wind_speed_10m_mean wind_mean,
                                        wind_speed_10m_max wind_max, precipitation_sum rain
                                        FROM weather_daily WHERE weather_point_id = ANY(:ids)
                                        ORDER BY weather_point_id, date"""), db.engine, params={"ids": chunk})
            df = df.dropna()
            out = []
            for pid, g in df.groupby("weather_point_id", sort=False):
                for d, f, p, dc_, isi_, bui_, fwi_ in fwi_lib.series(g.to_dict("records"), wind_unit):
                    out.append((pid, d, f, p, dc_, isi_, bui_, fwi_))
            res = pd.DataFrame(out, columns=["id", "date", "ffmc", "dmc", "dc", "isi", "bui", "fwi"])
            conn = db.engine.raw_connection()
            try:
                cur = conn.cursor()
                _copy_to_temp(cur, res, "tmp_fwi",
                              "id varchar, date date, ffmc float8, dmc float8, dc float8, isi float8, bui float8, fwi float8")
                cur.execute("""UPDATE weather_daily w SET ffmc=t.ffmc, dmc=t.dmc, dc=t.dc, isi=t.isi, bui=t.bui, fwi=t.fwi
                               FROM tmp_fwi t WHERE w.weather_point_id=t.id AND w.date=t.date""")
                conn.commit()
            finally:
                conn.close()
            done += len(res)
        _finish(run, records_read=done, records_updated=done)
        print(f"fwi computed for {done:,} point-days (wind unit: {wind_unit})")
    except Exception as exc:
        _fail(run, exc)


def build_features(start, end):
    call_procedure("CALL build_weather_features(%s, %s)", (start, end))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["points", "cellmap", "daily", "fwi", "features"])
    ap.add_argument("args", nargs="*")
    ap.add_argument("--wind-unit", choices=["ms", "kmh"], default="ms")
    a = ap.parse_args()
    with create_app().app_context():
        if a.step == "points":
            load_points(a.args[0])
        elif a.step == "cellmap":
            load_cellmap(a.args[0])
        elif a.step == "daily":
            load_daily(a.args[0])
        elif a.step == "fwi":
            compute_fwi(a.wind_unit)
        else:
            build_features(date.fromisoformat(a.args[0]), date.fromisoformat(a.args[1]))