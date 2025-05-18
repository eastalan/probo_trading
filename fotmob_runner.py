import os
import csv
import time
import datetime
import subprocess

FOTMOB_MATCHES_FILE = os.path.join("data", "event_data", "fotmob_matches.psv")
READ_WS_FRESH_SCRIPT = os.path.join(os.path.dirname(__file__), "read_ws_fresh.py")
POLL_INTERVAL_SECONDS = 30  # How often to check for new matches

def parse_ko_time(ko_time_str, match_date_str):
    """
    Parse KO time (e.g. '4:30 PM') and match date (YYYYMMDD) to a datetime object.
    """
    try:
        dt_str = f"{match_date_str} {ko_time_str.upper().replace('.', '')}"
        return datetime.datetime.strptime(dt_str, "%Y%m%d %I:%M %p")
    except Exception:
        try:
            # Try without AM/PM
            dt_str = f"{match_date_str} {ko_time_str}"
            return datetime.datetime.strptime(dt_str, "%Y%m%d %H:%M")
        except Exception:
            return None

def already_running(match_id, running_workers):
    return match_id in running_workers

def main():
    running_workers = {}  # match_id: subprocess.Popen

    while True:
        now = datetime.datetime.now()
        if not os.path.exists(FOTMOB_MATCHES_FILE):
            print(f"Match file not found: {FOTMOB_MATCHES_FILE}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        # Read all rows into memory for update logic
        with open(FOTMOB_MATCHES_FILE, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f, delimiter="|"))
            fieldnames = reader[0].keys() if reader else []

        updated_rows = []
        for row in reader:
            match_date = row.get("MatchDate")
            ko_time = row.get("KickOffTime")
            match_id = row.get("MatchID")
            match_link = row.get("MatchLink")
            download_flag = row.get("DownloadFlag", "0")
            has_ended = row.get("HasEnded", "0")

            # Ignore rows where hasEnded is 1
            if has_ended == "1":
                updated_rows.append(row)
                continue

            # Parse KO time
            ko_dt = parse_ko_time(ko_time, match_date)
            if not ko_dt:
                updated_rows.append(row)
                continue

            # If current time is 3h30m after KO and hasEnded is 0, set hasEnded to 1
            if (now - ko_dt).total_seconds() > 3.5 * 3600:
                row["HasEnded"] = "1"
                updated_rows.append(row)
                continue

            # If KO time is in the past, no worker running, and hasEnded is 0, launch worker
            if ko_dt <= now and not already_running(match_id, running_workers) and row["HasEnded"] == "0":
                print(f"Spawning worker for match {match_id}: {row['HomeTeam']} vs {row['AwayTeam']} ({ko_time})")
                env = os.environ.copy()
                env["FOTMOB_MATCH_URL"] = match_link
                env["HEADLESS_BROWSER"] = "True"
                proc = subprocess.Popen(
                    ["python3", READ_WS_FRESH_SCRIPT, "--headless"],
                    env=env
                )
                running_workers[match_id] = proc

            updated_rows.append(row)

        # Clean up finished workers
        finished = [mid for mid, proc in running_workers.items() if proc.poll() is not None]
        for mid in finished:
            print(f"Worker for match {mid} finished.")
            del running_workers[mid]

        # Write back any HasEnded updates
        if updated_rows and fieldnames:
            with open(FOTMOB_MATCHES_FILE, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="|")
                writer.writeheader()
                for row in updated_rows:
                    writer.writerow(row)

        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()