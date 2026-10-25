from app.extensions import db


class DailyFeature(db.Model):
    """One row per (cell, day). The table is PARTITIONED BY RANGE (date) in Postgres (see db/sql/001_mvp.sql);
    SQLAlchemy sees it as an ordinary table. Each ingest job owns its own columns and upserts only those."""
    
    __tablename__ = "daily_features"

    cell_id = db.Column(db.String, db.ForeignKey("cells.cell_id"), primary_key=True)
    date = db.Column(db.Date, primary_key=True, index=True)

    # NDVI job
    ndvi = db.Column(db.Float)
    ndvi_obs_date = db.Column(db.Date)        # composite date actually used (as-of join, avoids leakage)
    ndmi = db.Column(db.Float)
    # weather job
    fwi = db.Column(db.Float)
    temp_c = db.Column(db.Float)
    rainfall_mm = db.Column(db.Float)
    wind_speed = db.Column(db.Float)
    wind_dir = db.Column(db.Float)
    weather_method = db.Column(db.String)
    # label job (fire_events -> per-cell-day)
    fire_count = db.Column(db.Integer)
    max_frp = db.Column(db.Float)