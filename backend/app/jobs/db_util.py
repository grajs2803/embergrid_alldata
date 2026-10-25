from app.extensions import db


def call_procedure(sql, params):
    """Run CALL <procedure>(...) on a driver connection in autocommit mode (procedures that COMMIT cannot run inside a
    transaction block). The pooled connection goes back to the pool with autocommit RESTORED: leaving it on would make the
    next borrower's 'CREATE TEMP TABLE ... ON COMMIT DROP' vanish immediately."""
    conn = db.engine.raw_connection()
    raw = conn.driver_connection
    try:
        raw.autocommit = True
        raw.cursor().execute(sql, params)
    finally:
        raw.autocommit = False
        conn.close()