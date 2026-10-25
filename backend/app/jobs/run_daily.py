"""The daily pipeline. One command, safe to run any time, any number of times:

    python -m app.jobs.run_daily                      # target = today (IST)
    python -m app.jobs.run_daily --date 2026-10-24    # a specific day
    python -m app.jobs.run_daily --skip fire,ndvi     # skip steps
    python -m app.jobs.run_daily --date 2026-10-24 --backfill-days 120   # first live run: 120 days ending that date

Steps (each idempotent): scope -> fire -> weather -> labels -> ndvi -> weather features -> predict -> checks.
Self-healing: the window starts 5 days back, or 2 days before the last successful run, whichever is earlier (max 30 days),
so a missed day is filled on the next run. A failing step stops the run, raises an alert and exits with code 1.

Environment:
  FIRMS_MAP_KEY        NASA FIRMS key (required unless --skip fire)
  FIRMS_SOURCE         default VIIRS_NOAA20_NRT
  OPENMETEO_WIND_UNIT  ms | kmh. MUST match the unit of your historical weather files (default ms)
  ARCHIVE_LAG_DAYS     ERA5 archive publishes ~5 days late; newer days come from the forecast API (default 6)
  NDVI_DIR             folder with india_ndvi_<year>.tif; if unset, NDVI extraction is skipped
  ALERT_WEBHOOK_URL    optional Slack-compatible webhook, POSTed {"text": ...} on failures and warnings
"""
import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests

from app import create_app
from app.extensions import db
from app.jobs import live, load_fire, load_ndvi, load_weather, predict_day
from app.jobs.db_util import call_procedure
from app.ml import get_model
from app.models.ingestion_run import IngestionRun

LOOKBACK_DAYS = 5
MAX_HEAL_DAYS = 30
NDVI_STALE_DAYS = 40
NULL_SHARE_WARN = 0.05
STEPS = ["scope", "fire", "weather", "labels", "ndvi", "features", "predict", "checks"]


def today_ist():
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()


def alert(msg, level="ERROR"):
    print(f"[{level}] {msg}", file=sys.stderr)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        try:
            requests.post(url, json={"text": f"[EmberGrid {level}] {msg}"}, timeout=10)
        except requests.RequestException as exc:
            print(f"[WARN] alert webhook failed: {exc}", file=sys.stderr)


def window(D, backfill_days=None):
    if backfill_days:                                  # explicit one-off backfill: no cap, no heuristics
        return D - timedelta(days=backfill_days - 1)
    start = D - timedelta(days=LOOKBACK_DAYS - 1)
    last_ok = db.session.scalar(db.text("SELECT max(end_date) FROM ingestion_runs WHERE job_type='run_daily' AND status='success'"))
    if last_ok:
        start = min(start, last_ok - timedelta(days=2))
    return max(start, D - timedelta(days=MAX_HEAL_DAYS))


def scope_bbox():
    r = db.session.execute(db.text("""SELECT min(c.lon), min(c.lat), max(c.lon), max(c.lat), count(*)
                                      FROM cells c JOIN ingest_scope s USING (district)""")).one()
    if not r[4]:
        raise RuntimeError("ingest_scope selects no cells")
    pad = 0.05
    return (r[0] - pad, r[1] - pad, r[2] + pad, r[3] + pad)


def scope_points():
    return pd.read_sql(db.text("""SELECT DISTINCT p.weather_point_id, p.lat, p.lon FROM weather_points p
                                  JOIN cell_weather_map m USING (weather_point_id)
                                  JOIN cells c ON c.cell_id = m.cell_id
                                  JOIN ingest_scope s ON s.district = c.district ORDER BY 1"""), db.engine)


# ------------------------------------------------------------------ steps
def step_scope(D, start, ctx):
    n = db.session.execute(db.text("UPDATE ingest_scope SET to_date = :d WHERE rolling AND to_date < :d"), {"d": D}).rowcount
    db.session.commit()
    rolling = db.session.scalar(db.text("SELECT count(*) FROM ingest_scope WHERE rolling"))
    if not rolling:
        alert("no ingest_scope district is flagged rolling: new days will not get rows", "WARN")
    return f"{n} districts advanced to {D}, {rolling} rolling"


def step_fire(D, start, ctx):
    key = os.environ.get("FIRMS_MAP_KEY")
    if not key:
        raise RuntimeError("FIRMS_MAP_KEY is not set (use --skip fire to run without hotspots)")
    source = os.environ.get("FIRMS_SOURCE", "VIIRS_NOAA20_NRT")
    df = live.fetch_firms(key, source, scope_bbox(), start, D)
    seen, new = load_fire.ingest_firms_df(df, source)
    ctx["fire_new"] = new
    return f"{seen} detections in feed, {new} new"


def step_weather(D, start, ctx):
    pts = scope_points()
    if pts.empty:
        raise RuntimeError("no weather points cover the scope (load cell_weather_map first)")
    unit = os.environ.get("OPENMETEO_WIND_UNIT", "ms")
    today = ctx["today"]
    final_end = today - timedelta(days=int(os.environ.get("ARCHIVE_LAG_DAYS", 6)))
    # re-fetch a wider tail from the archive so earlier provisional forecast values are replaced by final ERA5
    a_start = start - timedelta(days=5)
    n_a = n_f = 0
    if a_start <= min(D, final_end):
        n_a = load_weather.upsert_weather_df(live.fetch_openmeteo(pts, a_start, min(D, final_end), "archive", unit), "era5")
    f_start = final_end + timedelta(days=1)        # forecast takes over exactly where the archive ends: no gap
    if f_start <= D:
        n_f = load_weather.upsert_weather_df(live.fetch_openmeteo(pts, f_start, D, "forecast", unit, today=today), "forecast")
    load_weather.compute_fwi(unit, point_ids=list(pts.weather_point_id))
    return f"{len(pts)} points: {n_a} era5 rows, {n_f} forecast rows, FWI recomputed"


def step_labels(D, start, ctx):
    call_procedure("CALL build_label_layer(%s, %s)", (start, D))
    return f"label layer {start}..{D}"


def step_ndvi(D, start, ctx):
    d = os.environ.get("NDVI_DIR")
    done = []
    if d:
        for y in sorted({start.year, D.year - 1, D.year}):
            f = os.path.join(d, f"india_ndvi_{y}.tif")
            if not os.path.exists(f):
                continue
            # skip a file that has not changed since its last successful extraction, unless some scope cell has no
            # composites at all (new district added to the scope)
            last = db.session.scalar(db.text("""SELECT max(finished_at) FROM ingestion_runs WHERE job_type='ndvi_composites'
                                                AND status='success' AND source=:s"""), {"s": os.path.basename(f)[:40]})
            missing = db.session.scalar(db.text("""SELECT count(*) FROM cells c JOIN ingest_scope s USING (district)
                          WHERE NOT EXISTS (SELECT 1 FROM ndvi_composites n WHERE n.cell_id = c.cell_id)"""))
            fresh = last is not None and datetime.fromtimestamp(os.path.getmtime(f), timezone.utc) < last
            if fresh and not missing:
                continue
            load_ndvi.extract(f)
            done.append(y)
    load_ndvi.features(start, D, 18)
    newest = db.session.scalar(db.text("SELECT max(obs_date) FROM ndvi_composites WHERE ndvi IS NOT NULL"))
    if newest is None or (D - newest).days > NDVI_STALE_DAYS:
        alert(f"NDVI is stale: newest composite starts {newest}, run date {D} (export a new composite and re-run)", "WARN")
    return f"extracted years {done or 'none'}, newest composite {newest}"


def step_features(D, start, ctx):
    load_weather.build_features(start, D)
    return f"weather copied {start}..{D}"


def step_predict(D, start, ctx):
    job = predict_day.run(start, D)
    if job.status != "success":
        raise RuntimeError(f"predict_day failed: {job.error_message}")
    ctx["predicted"] = job.records_read
    return f"{job.records_read} predictions ({get_model().version})"


def step_checks(D, start, ctx):
    """Staleness guard: a model input that is silently NULL looks like normal output (it becomes the training mean)."""
    allowed = {"fwi", "ndvi", "rainfall_mm", "temp_c", "wind_speed"}
    feats = [f for f in get_model().feature_list if f in allowed]
    q = "SELECT count(*), " + ", ".join(f"count({f})" for f in feats) + " FROM daily_features WHERE date = :d"
    row = db.session.execute(db.text(q), {"d": D}).one()
    total, msgs = row[0], []
    if not total:
        raise RuntimeError(f"no daily_features rows for {D}: nothing to serve")
    for f, c in zip(feats, row[1:]):
        share = 1 - c / total
        if share > NULL_SHARE_WARN:
            msgs.append(f"{f} is NULL for {share:.0%} of {D}")
    if msgs:
        alert("; ".join(msgs), "WARN")
    return "ok" if not msgs else "warnings: " + "; ".join(msgs)


FUNCS = dict(scope=step_scope, fire=step_fire, weather=step_weather, labels=step_labels, ndvi=step_ndvi,
             features=step_features, predict=step_predict, checks=step_checks)


def run(D=None, skip=(), today=None, backfill_days=None):
    D = D or today_ist()
    today = today or today_ist()
    start = window(D, backfill_days)
    parent = IngestionRun(source="pipeline", job_type="run_daily", start_date=start, end_date=D, status="running")
    db.session.add(parent)
    db.session.commit()
    ctx, log = {"today": today}, []
    print(f"run_daily target {D}, window {start}..{D}")
    for name in STEPS:
        if name in skip:
            log.append((name, "skipped"))
            print(f"  {name:9s} skipped")
            continue
        t0 = time.time()
        try:
            msg = FUNCS[name](D, start, ctx)
        except Exception as exc:
            db.session.rollback()
            parent.status, parent.finished_at = "failed", datetime.now(timezone.utc)
            parent.error_message = f"step '{name}': {exc}"[:500]
            db.session.commit()
            alert(f"daily run for {D} failed at step '{name}': {exc}")
            raise SystemExit(1)
        print(f"  {name:9s} {msg}  ({time.time() - t0:.1f}s)")
        log.append((name, msg))
    parent.status, parent.finished_at = "success", datetime.now(timezone.utc)
    parent.records_inserted, parent.records_updated = ctx.get("fire_new", 0), ctx.get("predicted", 0)
    db.session.commit()
    print("run_daily success")
    return log


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=date.fromisoformat)
    ap.add_argument("--skip", default="")
    ap.add_argument("--backfill-days", type=int, default=None,
                    help="process this many days ending at --date (use ~120 on the FIRST live run so FWI has a spin-up)")
    a = ap.parse_args()
    bad = set(filter(None, a.skip.split(","))) - set(STEPS)
    if bad:
        raise SystemExit(f"unknown steps {sorted(bad)}; valid: {STEPS}")
    with create_app().app_context():
        run(a.date, set(filter(None, a.skip.split(","))), backfill_days=a.backfill_days)