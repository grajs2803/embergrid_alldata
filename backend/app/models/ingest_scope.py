from datetime import datetime, timezone

from app.extensions import db


class IngestScope(db.Model):
    """What is loaded. A daily_features row exists only for (district, date) inside its scope window.
    Absent row = 'not loaded' (unknown), never 'no fire'. Grow coverage by adding rows / widening dates."""
    __tablename__ = "ingest_scope"

    district = db.Column(db.String, primary_key=True)
    from_date = db.Column(db.Date, nullable=False)
    to_date = db.Column(db.Date, nullable=False)
    added_at = db.Column(db.DateTime(timezone=True), nullable=False,
                         default=lambda: datetime.now(timezone.utc))