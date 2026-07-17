import os
import json
import sqlite3
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta

from flask import Flask, jsonify, request, send_from_directory, Response
from werkzeug.utils import secure_filename
import pandas as pd

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500 MB max upload

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")   # scratch space for raw uploaded files, wiped right after parsing
STORAGE_DIR = os.path.join(BASE_DIR, "storage")     # persistent — must be a real disk, NOT /tmp (that's why we dropped the Vercel path)
DB_PATH = os.path.join(STORAGE_DIR, "rejections.db")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(STORAGE_DIR, exist_ok=True)

MAX_FILES = 100
RETENTION_DAYS = 60  # ~2 months, rolling window keyed on each record's OWN event timestamp


# ---------------------------------------------------------------------------
# Storage layer
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Defensive: ensure the schema exists on every single connection, not just once at import
    # time. This is cheap (IF NOT EXISTS) and guards against any deployment scenario where the
    # DB file gets reset, recreated on a fresh volume, or opened by a process that skipped the
    # one-time init — all of which produce exactly "no such table: rejections".
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rejections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            machine_name TEXT NOT NULL,
            reject_detail TEXT,
            message TEXT,
            hour INTEGER,
            job_name TEXT,
            ingested_at TEXT NOT NULL,
            UNIQUE(timestamp, machine_name, reject_detail, message, job_name)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON rejections(timestamp)")
    conn.commit()
    return conn


def init_db():
    # Kept as an explicit startup call (see bottom of this section) so schema creation happens
    # once eagerly and errors surface immediately at boot rather than on first request.
    with closing(get_db()):
        pass


init_db()
logging.basicConfig(level=logging.INFO)
logging.info(f"Storage DB path: {DB_PATH} (exists: {os.path.exists(DB_PATH)})")


def prune_old_records(conn):
    """Delete rows whose OWN event timestamp is older than the retention window — a rolling
    2-month log, not a 2-month-since-upload rule."""
    cutoff = (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute("DELETE FROM rejections WHERE timestamp < ?", (cutoff,))
    conn.commit()


def insert_records(conn, records):
    """Insert cleaned rows, silently skipping exact duplicates (e.g. re-uploading a file that
    overlaps a previous upload's date range). This is a safety net only — it is not a
    substitute for the Case 1/2/3 dedup logic that already ran on the file before it got here."""
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    conn.executemany(
        """INSERT OR IGNORE INTO rejections
           (timestamp, machine_name, reject_detail, message, hour, job_name, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (r.get("Timestamp"), r.get("Machine Name"), r.get("Reject Detail"),
             r.get("Message"), r.get("Hour"), r.get("Job Name"), now)
            for r in records
        ],
    )
    conn.commit()


def fetch_all_records(conn):
    rows = conn.execute(
        "SELECT timestamp, machine_name, reject_detail, message, hour, job_name "
        "FROM rejections ORDER BY timestamp"
    ).fetchall()
    return [
        {
            "Timestamp": r["timestamp"],
            "Machine Name": r["machine_name"],
            "Reject Detail": r["reject_detail"],
            "Message": r["message"],
            "Hour": r["hour"],
            "Job Name": r["job_name"],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# File reading / cleaning
# ---------------------------------------------------------------------------

def read_file_to_df(filepath):
    """Read a file into a DataFrame based on its extension."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        return pd.read_csv(filepath)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(filepath, engine="openpyxl")
    elif ext == ".ods":
        return pd.read_excel(filepath, engine="odf")
    else:
        raise ValueError(f"Unsupported file format: {ext}")


def clean_data(df):
    """Case 1/2/3 dedup pipeline.

    Case 1 (Job Name + Nr Order both present): unchanged — (Job Name, Nr Order) together is a
    true unique job-instance key, confirmed against real data (many Job Names legitimately repeat
    across different Nr Orders), so an exact-match drop_duplicates is correct regardless of the
    time gap between repeats.

    Case 2 (Job Name present, Nr Order missing): machine-aware windowed dedup.
        - Same machine as the last kept row, within the day/night window -> duplicate log spam -> drop.
        - Different machine, within the window -> the job failed and got transferred -> KEEP
          (a real, distinct fault for that machine), and this does NOT count as a new job instance —
          it's still the same job execution, just continuing elsewhere.
        - Same machine, gap exceeds the window -> looks like Job Name being reused for a genuinely
          new job -> only allowed if fewer than 2 such new instances started in the trailing 24h
          (reuse that fast is rare, per the operator's own description of how jobs work).

    Case 3 (both missing): no Job Name/Order at all, so no job-instance concept and no transfer
    logic applies. Just a straightforward day/night-window debounce keyed on
    (Machine Name, Reject Detail, Message) to collapse rapid retriggers of the identical fault.
    """
    df.columns = df.columns.str.strip()
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce", format="%m/%d/%Y %I:%M:%S %p")

    df_r = df[df["Machine Name"].fillna("").str.startswith("Racer")].copy()
    df_r = df_r[df_r["Reject Detail"] != "Twin lens rejected"].copy()
    df_r = df_r.sort_values("DateTime").reset_index(drop=True)

    job_missing = (
        df_r["Job Name"].fillna("").astype(str).str.strip()
        .replace(["?", "---", "nan", "None"], "").eq("")
    )
    order_missing = (
        df_r["Nr Order"].isna()
        | df_r["Nr Order"].astype(str).str.strip().isin(["", "nan", "None"])
    )

    is_c1 = (~job_missing) & (~order_missing)
    is_c2 = (~job_missing) & (order_missing)
    is_c3 = (job_missing) & (order_missing)

    # ---- Case 1 ----
    df_c1_clean = df_r[is_c1].drop_duplicates(subset=["Job Name", "Nr Order"], keep="first")

    WINDOW_DAY = pd.Timedelta(hours=7.0)
    WINDOW_NIGHT = pd.Timedelta(hours=14)
    DAY_LIMIT = pd.Timedelta(hours=24)

    # ---- Case 2: machine-aware windowed dedup ----
    accepted_rows_c2 = []
    c2_sub = df_r.loc[is_c2, ["DateTime", "Machine Name"]]
    job_names_c2 = df_r.loc[is_c2, "Job Name"]
    for job_name, grp in c2_sub.groupby(job_names_c2, sort=False):
        grp = grp.sort_values("DateTime")
        last_kept_machine = None
        last_kept_time = None
        instance_times = []
        # zip over Index + column values avoids the per-row Series construction cost of iterrows
        for idx, dt, machine in zip(grp.index, grp["DateTime"], grp["Machine Name"]):
            if last_kept_time is None:
                accepted_rows_c2.append(idx)
                instance_times = [dt]
                last_kept_machine, last_kept_time = machine, dt
                continue

            window = WINDOW_NIGHT if last_kept_time.hour >= 20 else WINDOW_DAY
            gap = dt - last_kept_time

            if machine == last_kept_machine and gap <= window:
                # same machine, still inside the debounce window -> duplicate
                continue

            if machine != last_kept_machine:
                # transferred to a different machine -> real, distinct fault for that machine;
                # doesn't consume the 24h new-instance cap since it's the same job execution
                accepted_rows_c2.append(idx)
                last_kept_machine, last_kept_time = machine, dt
                continue

            # same machine, gap exceeds window -> candidate for a genuinely new job instance
            instance_times = [t for t in instance_times if dt - t < DAY_LIMIT]
            if len(instance_times) >= 2:
                continue
            accepted_rows_c2.append(idx)
            instance_times.append(dt)
            last_kept_machine, last_kept_time = machine, dt

    df_c2_clean = df_r.loc[accepted_rows_c2]

    # ---- Case 3: window-debounce on (Machine Name, Reject Detail, Message) ----
    accepted_rows_c3 = []
    c3_sub = df_r.loc[is_c3, ["DateTime"]]
    group_keys_c3 = df_r.loc[is_c3, ["Machine Name", "Reject Detail", "Message"]]
    for key, grp in c3_sub.groupby(
        [group_keys_c3["Machine Name"], group_keys_c3["Reject Detail"], group_keys_c3["Message"]],
        sort=False,
    ):
        grp = grp.sort_values("DateTime")
        last_kept_time = None
        for idx, dt in zip(grp.index, grp["DateTime"]):
            if last_kept_time is None:
                accepted_rows_c3.append(idx)
                last_kept_time = dt
                continue
            window = WINDOW_NIGHT if last_kept_time.hour >= 20 else WINDOW_DAY
            if dt - last_kept_time <= window:
                continue
            accepted_rows_c3.append(idx)
            last_kept_time = dt

    df_c3_clean = df_r.loc[accepted_rows_c3]

    final_df = pd.concat([df_c1_clean, df_c2_clean, df_c3_clean]).sort_values("DateTime")
    final_df["Timestamp"] = final_df["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    final_df["Hour"] = final_df["DateTime"].dt.hour

    return final_df


def _save_and_read(file_tuple):
    """Save an uploaded file to disk, read it into a DataFrame, then clean up the raw file."""
    file_obj, upload_folder = file_tuple
    raw_name = secure_filename(file_obj.filename)
    if not raw_name:
        raise ValueError("Invalid filename")
    filename = f"{uuid.uuid4().hex}_{raw_name}"
    filepath = os.path.join(upload_folder, filename)
    file_obj.save(filepath)
    try:
        return read_file_to_df(filepath)
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)


def fast_json_response(obj):
    try:
        import orjson
        return Response(orjson.dumps(obj), mimetype="application/json")
    except ImportError:
        return Response(json.dumps(obj, default=str), mimetype="application/json")


def _dataset_response():
    """Prune expired rows, then return the full currently-retained (<=60 day) dataset."""
    with closing(get_db()) as conn:
        prune_old_records(conn)
        records = fetch_all_records(conn)

    if not records:
        return fast_json_response({"data": [], "min_ts": "", "max_ts": ""})

    timestamps = [r["Timestamp"] for r in records if r["Timestamp"]]
    min_ts = min(timestamps)[:16]
    max_dt = pd.to_datetime(max(timestamps)) + pd.Timedelta(minutes=1)
    max_ts = max_dt.strftime("%Y-%m-%dT%H:%M")
    return fast_json_response({"data": records, "min_ts": min_ts, "max_ts": max_ts})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def home():
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/data", methods=["GET"])
def get_data():
    """Returns whatever is currently retained — no upload needed. Powers the initial page load
    so a returning user sees past data without re-uploading anything."""
    try:
        return _dataset_response()
    except Exception:
        logging.exception("Fetch stored data error")
        return jsonify({"error": "Could not load stored data"}), 500


@app.route("/api/analyze", methods=["POST"])
def analyze_file():
    """Process uploaded files and return cleaned data WITHOUT storing to DB.
    This is a preview-only endpoint — data disappears on page refresh."""
    files = request.files.getlist("file")
    if not files or all(f.filename == "" for f in files):
        return jsonify({"error": "No file provided"}), 400

    files = [f for f in files if f.filename != ""]
    if len(files) > MAX_FILES:
        return jsonify({"error": f"Too many files. Maximum is {MAX_FILES}."}), 400

    try:
        if len(files) == 1:
            df = _save_and_read((files[0], UPLOAD_FOLDER))
        else:
            with ThreadPoolExecutor(max_workers=min(len(files), 8)) as pool:
                dfs = list(pool.map(_save_and_read, [(f, UPLOAD_FOLDER) for f in files]))
            df = pd.concat(dfs, ignore_index=True)

        final_df = clean_data(df)

        records = (
            final_df[["Timestamp", "Machine Name", "Reject Detail", "Message", "Hour", "Job Name"]]
            .dropna(subset=["Timestamp", "Machine Name"])
            .to_dict(orient="records")
        )

        if not records:
            return fast_json_response({"data": [], "min_ts": "", "max_ts": ""})

        timestamps = [r["Timestamp"] for r in records if r["Timestamp"]]
        min_ts = min(timestamps)[:16]
        max_dt = pd.to_datetime(max(timestamps)) + pd.Timedelta(minutes=1)
        max_ts = max_dt.strftime("%Y-%m-%dT%H:%M")
        return fast_json_response({"data": records, "min_ts": min_ts, "max_ts": max_ts})

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except KeyError as e:
        return jsonify({"error": f"Missing required column: {e}"}), 400
    except Exception as e:
        logging.exception("Analyze processing error")
        return jsonify({"error": f"Processing error: {str(e)}"}), 500


@app.route("/api/upload", methods=["POST"])
def upload_file():
    """Process uploaded files, store cleaned data to DB, and return the full stored dataset."""
    files = request.files.getlist("file")
    if not files or all(f.filename == "" for f in files):
        return jsonify({"error": "No file provided"}), 400

    files = [f for f in files if f.filename != ""]
    if len(files) > MAX_FILES:
        return jsonify({"error": f"Too many files. Maximum is {MAX_FILES}."}), 400

    try:
        if len(files) == 1:
            df = _save_and_read((files[0], UPLOAD_FOLDER))
        else:
            with ThreadPoolExecutor(max_workers=min(len(files), 8)) as pool:
                dfs = list(pool.map(_save_and_read, [(f, UPLOAD_FOLDER) for f in files]))
            df = pd.concat(dfs, ignore_index=True)

        final_df = clean_data(df)

        new_records = (
            final_df[["Timestamp", "Machine Name", "Reject Detail", "Message", "Hour", "Job Name"]]
            .dropna(subset=["Timestamp", "Machine Name"])
            .to_dict(orient="records")
        )

        # Persist into the rolling 2-month store, then return the FULL current dataset —
        # existing retained rows plus this upload's new ones — not just what was just uploaded.
        with closing(get_db()) as conn:
            insert_records(conn, new_records)
            prune_old_records(conn)

        return _dataset_response()

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except KeyError as e:
        return jsonify({"error": f"Missing required column: {e}"}), 400
    except Exception as e:
        logging.exception("Upload processing error")
        return jsonify({"error": f"Processing error: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG", "false").lower() == "true", port=5001)