from datetime import datetime, timezone

from app.extensions import db


class IngestionRun(db.Model):
    """Audit log: one row per job chunk. status: running | success | failed."""
    __tablename__ = "ingestion_runs"

    id = db.Column(db.BigInteger, primary_key=True)
    source = db.Column(db.String, nullable=False)
    job_type = db.Column(db.String, nullable=False)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    status = db.Column(db.String, nullable=False)
    started_at = db.Column(db.DateTime(timezone=True), nullable=False,
                           default=lambda: datetime.now(timezone.utc))
    finished_at = db.Column(db.DateTime(timezone=True))
    records_read = db.Column(db.BigInteger, nullable=False, default=0)
    records_inserted = db.Column(db.BigInteger, nullable=False, default=0)
    records_updated = db.Column(db.BigInteger, nullable=False, default=0)
    records_rejected = db.Column(db.BigInteger, nullable=False, default=0)
    records_skipped = db.Column(db.BigInteger, nullable=False, default=0)
    error_message = db.Column(db.Text)