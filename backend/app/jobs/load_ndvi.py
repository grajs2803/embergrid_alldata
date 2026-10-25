"""Extract per-cell NDVI composites from yearly multiband GeoTIFFs (one band per 16-day composite) for the cells in
ingest_scope, then copy them onto daily_features with an as-of join (SQL procedure build_ndvi_features).

  python -m app.jobs.load_ndvi extract "/path/NDVI_DATA/india_ndvi_*.tif"
  python -m app.jobs.load_ndvi features 2020-02-01 2020-04-30 [--avail-days 18]

Raster facts this relies on (from your files): EPSG:4326, ~0.00898 deg pixels, band descriptions like 2020_02_18_NDVI.
Cells (0.01 deg) and pixels (0.00898 deg) do not align, so each cell centre is read from the pixel that contains it
(nearest-pixel sampling; NOT an average of the cell's area).
"""
import argparse
import glob
import io
import re
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window

from app import create_app
from app.extensions import db
from app.jobs.db_util import call_procedure
from app.models.ingestion_run import IngestionRun

BAND_RE = re.compile(r"(\d{4})_(\d{2})_(\d{2})")


def band_dates(src):
    out = []
    for i, d in enumerate(src.descriptions, start=1):
        m = BAND_RE.search(d or "")
        if not m:
            raise ValueError(f"band {i} of {src.name} has no date in its description: {d!r}")
        out.append(date(int(m[1]), int(m[2]), int(m[3])))
    return out


def sample_cells(src, lats, lons):
    """Return (values[bands, n_cells], inside_mask). Pixel = floor of the affine inverse of the cell centre."""
    t = src.transform
    cols = np.floor((lons - t.c) / t.a).astype(int)
    rows = np.floor((lats - t.f) / t.e).astype(int)
    inside = (rows >= 0) & (rows < src.height) & (cols >= 0) & (cols < src.width)
    vals = np.full((src.count, len(lats)), np.nan, dtype="float32")
    if inside.any():
        r0, r1, c0, c1 = rows[inside].min(), rows[inside].max(), cols[inside].min(), cols[inside].max()
        block = src.read(window=Window(c0, r0, c1 - c0 + 1, r1 - r0 + 1)).astype("float32")   # (bands, h, w)
        vals[:, inside] = block[:, rows[inside] - r0, cols[inside] - c0]
    return vals, inside


def clean(vals, name):
    """NDVI must live in [-1, 1]. If it looks like raw MODIS integers (x10000), rescale; anything outside -> NULL."""
    peak = np.nanmax(np.abs(vals)) if np.isfinite(vals).any() else 0.0
    if peak > 2.0:
        print(f"  {name}: max |value| = {peak:.0f} -> looks like integer-scaled NDVI, multiplying by 0.0001")
        vals = vals * 0.0001
    bad = ~np.isfinite(vals) | (vals < -1.0) | (vals > 1.0)
    vals = np.where(bad, np.nan, vals)
    return vals, float(bad.mean())


def _run(source, job_type, **kw):
    run = IngestionRun(source=source.replace("\\", "/").split("/")[-1][:40], job_type=job_type, status="running", **kw)
    db.session.add(run)
    db.session.commit()
    return run


def extract(pattern):
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"no files match {pattern}")
    cells = pd.read_sql(db.text("SELECT c.cell_id, c.lat, c.lon FROM cells c JOIN ingest_scope s USING (district)"), db.engine)
    if cells.empty:
        raise SystemExit("ingest_scope selects no cells; insert districts first")
    print(f"{len(cells):,} scope cells, {len(files)} raster files")
    lats, lons = cells.lat.values, cells.lon.values
    total = 0
    for f in files:
        run = _run(f, "ndvi_composites")
        try:
            with rasterio.open(f) as src:
                dates = band_dates(src)
                vals, inside = sample_cells(src, lats, lons)
            vals, bad_share = clean(vals, f.split("/")[-1])
            ids = cells.cell_id.values[inside]
            v = vals[:, inside]
            df = pd.DataFrame({"cell_id": np.tile(ids, len(dates)),
                               "obs_date": np.repeat(dates, len(ids)),
                               "ndvi": v.reshape(-1)})
            conn = db.engine.raw_connection()
            try:
                cur = conn.cursor()
                cur.execute("CREATE TEMP TABLE tmp_nd (cell_id varchar, obs_date date, ndvi real) ON COMMIT DROP")
                buf = io.StringIO()
                df.to_csv(buf, index=False, header=False, na_rep="\\N")
                buf.seek(0)
                cur.copy_expert("COPY tmp_nd FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
                cur.execute("""INSERT INTO ndvi_composites (cell_id, obs_date, ndvi) SELECT cell_id, obs_date, ndvi FROM tmp_nd
                               ON CONFLICT (cell_id, obs_date) DO UPDATE SET ndvi = EXCLUDED.ndvi""")
                conn.commit()
            finally:
                conn.close()
            run.records_read, run.records_inserted = len(df), len(df)
            run.records_rejected = int((~inside).sum()) * len(dates)
            run.status, run.finished_at = "success", datetime.now(timezone.utc)
            db.session.commit()
            total += len(df)
            print(f"  {f.split('/')[-1]}: {len(dates)} composites, {int((~inside).sum())} cells outside raster, "
                  f"{bad_share:.1%} invalid/masked values")
        except Exception as exc:
            db.session.rollback()
            run.status, run.finished_at, run.error_message = "failed", datetime.now(timezone.utc), str(exc)[:500]
            db.session.commit()
            raise
    print(f"extracted {total:,} cell-composite values")


def features(start, end, avail_days):
    call_procedure("CALL build_ndvi_features(%s, %s, %s)", (start, end, avail_days))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["extract", "features"])
    ap.add_argument("args", nargs="+")
    ap.add_argument("--avail-days", type=int, default=18)
    a = ap.parse_args()
    with create_app().app_context():
        if a.step == "extract":
            extract(a.args[0])
        else:
            features(date.fromisoformat(a.args[0]), date.fromisoformat(a.args[1]), a.avail_days)