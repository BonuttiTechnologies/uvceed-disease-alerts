from .db import db_conn, ensure_phase3_schema

def main():
    with db_conn() as conn:
        ensure_phase3_schema(conn)
    print("OK: Phase 3 schema ensured (signal_snapshots + zip_requests + indexes).")

if __name__ == "__main__":
    main()
