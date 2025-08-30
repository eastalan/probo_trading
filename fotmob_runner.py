import os
import csv
import time
import datetime
import subprocess
import pdb
import re
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from process_utils import kill_old_socket_processes

FOTMOB_MATCHES_FILE = os.path.join("data", "event_data", "fotmob_matches.psv")
FOTMOB_SOCKET_SCRIPT = os.path.join(os.path.dirname(__file__), "fotmob_socket.py")
FOTMOB_UUID_EXTRACTOR = os.path.join(os.path.dirname(__file__), "fotmob_uuid_extractor.py")
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

def extract_event_uuid_via_subprocess(match_url, match_id):
    """
    Extract EVENT_UUID using the separate fotmob_uuid_extractor.py program.
    """
    try:
        print(f"Extracting UUID for match {match_id} using separate extractor...")
        result = subprocess.run(
            ["python3", FOTMOB_UUID_EXTRACTOR, match_url, "--match-id", match_id],
            capture_output=True,
            text=True,
            timeout=300  # Increased timeout for retry logic
        )
        
        if result.returncode == 0:
            # Parse the UUID from the output
            for line in result.stdout.split('\n'):
                if "SUCCESS: EVENT_UUID =" in line:
                    event_uuid = line.split("=")[-1].strip()
                    print(f"Successfully extracted UUID: {event_uuid}")
                    return event_uuid
            
            print("UUID extraction succeeded but could not parse result")
            return None
        else:
            print(f"UUID extraction failed: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        print(f"UUID extraction timed out for match {match_id}")
        return None
    except Exception as e:
        print(f"Error running UUID extractor: {e}")
        return None

def process_match_uuid_and_socket(match_data, df, running_workers, temp_files):
    """
    Process a single match: extract UUID, save to PSV, and spawn socket if successful.
    Returns tuple: (match_id, success, event_uuid)
    """
    idx, row = match_data
    match_id = row['MatchID']
    match_link = f"https://www.fotmob.com/en-GB/match/{match_id}/playbyplay"
    
    try:
        print(f"Processing match {match_id}: {row['HomeTeam']} vs {row['AwayTeam']} ({row['KickOffTime']})")
        
        # Check if UUID exists for this match
        current_uuid = row.get("UUID", "")
        event_uuid = None
        
        # Handle pandas NaN values properly - extract UUID if missing/invalid
        if pd.isna(current_uuid) or str(current_uuid).strip() in ["", "nan", "NaN"]:
            print(f"No valid UUID found for match {match_id}, extracting...")
            
            # Extract EVENT_UUID using separate program
            event_uuid = extract_event_uuid_via_subprocess(match_link, match_id)
            if event_uuid:
                # Update UUID in the DataFrame (thread-safe update)
                df.loc[idx, "UUID"] = event_uuid
                # Save the DataFrame immediately after updating UUID
                df.to_csv(FOTMOB_MATCHES_FILE, sep="|", index=False)
                print(f"Extracted and saved UUID for match {match_id}: {event_uuid}")
            else:
                print(f"Could not extract UUID for match {match_id}")
                return (match_id, False, None)
        else:
            event_uuid = str(current_uuid).strip()
            # Double-check that the UUID is not invalid
            if event_uuid in ["", "nan", "NaN"]:
                print(f"Invalid UUID '{event_uuid}' for match {match_id}")
                return (match_id, False, None)
            print(f"Using existing UUID for match {match_id}: {event_uuid}")
        
        # Only spawn socket if we have a valid UUID and not already running
        if event_uuid and not already_running(match_id, running_workers):
            try:
                # Create temp directory for socket files
                temp_dir = os.path.join(os.path.dirname(__file__), "temp_sockets")
                os.makedirs(temp_dir, exist_ok=True)
                
                # Create a modified version of fotmob_socket.py for this match
                temp_socket_file = os.path.join(temp_dir, f"fotmob_socket_{match_id}.py")
                
                # Read the original fotmob_socket.py
                with open(FOTMOB_SOCKET_SCRIPT, 'r') as f:
                    socket_content = f.read()
                
                # Replace the hardcoded EVENT_UUID and MATCH_ID with the extracted ones
                modified_content = re.sub(
                    r'EVENT_UUID = "[^"]*"',
                    f'EVENT_UUID = "{event_uuid}"',
                    socket_content
                )
                modified_content = re.sub(
                    r'MATCH_ID = "[^"]*"',
                    f'MATCH_ID = "{match_id}"',
                    modified_content
                )
                
                # Fix import path for log_utils since we're in temp_sockets subdirectory
                modified_content = modified_content.replace(
                    'from log_utils import get_dated_log_path',
                    'import sys\nimport os\nsys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))\nfrom log_utils import get_dated_log_path'
                )
                
                # Write the modified script to the main directory
                with open(temp_socket_file, 'w') as f:
                    f.write(modified_content)
                
                temp_files.add(temp_socket_file)
                
                # Launch the modified socket script
                proc = subprocess.Popen(["python3", temp_socket_file])
                proc.start_time = time.time()  # Track start time for cleanup
                running_workers[match_id] = proc
                print(f"Launched fotmob_socket for match {match_id} with EVENT_UUID: {event_uuid}")
                
                # Update DownloadFlag to 1 to indicate processing started
                df.loc[idx, "DownloadFlag"] = "1"
                # Save the DataFrame immediately after updating DownloadFlag
                df.to_csv(FOTMOB_MATCHES_FILE, sep="|", index=False)
                print(f"Updated DownloadFlag to 1 for match {match_id}")
                
                return (match_id, True, event_uuid)
                
            except Exception as e:
                print(f"Error spawning socket for match {match_id}: {e}")
                return (match_id, False, event_uuid)
        elif event_uuid:
            print(f"Socket already running for match {match_id}")
            return (match_id, True, event_uuid)
        else:
            print(f"No valid UUID available for match {match_id}, cannot spawn socket")
            return (match_id, False, None)
            
    except Exception as e:
        print(f"Error processing match {match_id}: {e}")
        return (match_id, False, None)

def already_running(match_id, running_workers):
    return match_id in running_workers

def cleanup_temp_files():
    """
    Clean up temporary socket files.
    """
    try:
        # Clean up files in main directory (legacy)
        for file in os.listdir('.'):
            if file.startswith('fotmob_socket_') and file.endswith('.py'):
                os.remove(file)
                print(f"Cleaned up temporary file: {file}")
        
        # Clean up files in temp_sockets directory
        temp_dir = "temp_sockets"
        if os.path.exists(temp_dir):
            for file in os.listdir(temp_dir):
                if file.startswith('fotmob_socket_') and file.endswith('.py'):
                    os.remove(os.path.join(temp_dir, file))
                    print(f"Cleaned up temporary file: {os.path.join(temp_dir, file)}")
            # Remove empty directory
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass  # Directory not empty, that's fine
    except Exception as e:
        print(f"Error cleaning up temp files: {e}")

def main():
    running_workers = {}  # match_id: subprocess.Popen
    temp_files = set()  # Track temporary files for cleanup

    while True:
        now = datetime.datetime.now()
        today_str = now.strftime("%Y%m%d")
        if not os.path.exists(FOTMOB_MATCHES_FILE):
            print(f"Match file not found: {FOTMOB_MATCHES_FILE}")
            raise(FileNotFoundError(f"Match file not found: {FOTMOB_MATCHES_FILE}"))
        
        # Read data as DataFrame for efficient processing
        df = pd.read_csv(FOTMOB_MATCHES_FILE, delimiter="|", dtype=str)
        
        # Filter for today's matches only
        today_matches = df[df['MatchDate'] == today_str].copy()
        
        # Auto-expire matches: Set HasEnded=1 for matches >3.5 hours past KO time
        for idx, row in today_matches.iterrows():
            ko_dt = parse_ko_time(row['KickOffTime'], row['MatchDate'])
            if ko_dt and (now - ko_dt).total_seconds() > 3.5 * 3600:
                df.loc[idx, 'HasEnded'] = '1'
        
        # Reset matches with invalid UUIDs so they can be reprocessed
        for idx, row in today_matches.iterrows():
            current_uuid = row.get("UUID", "")
            if (row['DownloadFlag'] == '1' and row['HasEnded'] != '1' and 
                (pd.isna(current_uuid) or str(current_uuid).strip() in ["", "nan", "NaN"])):
                print(f"Resetting match {row['MatchID']} with invalid UUID for reprocessing")
                df.loc[idx, 'DownloadFlag'] = '0'
        
        # Get matches that need processing: started, not downloaded, not ended
        processable_matches = today_matches[
            (today_matches['HasEnded'] != '1') & 
            (today_matches['DownloadFlag'] == '0')
        ]
        
        # Process matches in parallel for UUID extraction and socket spawning
        matches_to_process = []
        for idx, row in processable_matches.iterrows():
            ko_dt = parse_ko_time(row['KickOffTime'], row['MatchDate'])
            if ko_dt and ko_dt <= now:
                matches_to_process.append((idx, row))
        
        if matches_to_process:
            print(f"Processing {len(matches_to_process)} started matches in parallel...")
            
            # Use ThreadPoolExecutor for parallel UUID extraction
            with ThreadPoolExecutor(max_workers=min(20, len(matches_to_process))) as executor:
                # Submit all matches for processing
                future_to_match = {
                    executor.submit(process_match_uuid_and_socket, match_data, df, running_workers, temp_files): match_data[1]['MatchID']
                    for match_data in matches_to_process
                }
                
                # Process completed futures as they finish
                for future in as_completed(future_to_match):
                    match_id = future_to_match[future]
                    try:
                        result_match_id, success, event_uuid = future.result()
                        if success:
                            print(f"Successfully processed match {result_match_id}")
                        else:
                            print(f"Failed to process match {result_match_id}")
                    except Exception as e:
                        print(f"Exception processing match {match_id}: {e}")
            
            # Save DataFrame after all parallel processing is complete
            df.to_csv(FOTMOB_MATCHES_FILE, sep="|", index=False)
            print("Saved all UUID updates to PSV file")

        # Clean up finished workers and their temp files
        finished = [mid for mid, proc in running_workers.items() if proc.poll() is not None]
        for mid in finished:
            print(f"Worker for match {mid} finished.")
            # Clean up the temporary socket file for this match
            temp_file = os.path.join("temp_sockets", f"fotmob_socket_{mid}.py")
            if temp_file in temp_files:
                try:
                    os.remove(temp_file)
                    temp_files.remove(temp_file)
                    print(f"Cleaned up temp file: {temp_file}")
                except Exception as e:
                    print(f"Error cleaning up {temp_file}: {e}")
            del running_workers[mid]
        
        # Kill inactive sockets (no data for >30 minutes)
        current_time = time.time()
        inactive_workers = []
        for mid, proc in running_workers.items():
            if proc.poll() is None:  # Still running
                # Check if log file has recent activity
                log_file = os.path.join("logs", "fotmob_data", now.strftime("%Y-%m-%d"), f"{mid}.log")
                if os.path.exists(log_file):
                    file_mod_time = os.path.getmtime(log_file)
                    if current_time - file_mod_time > 1200:  # 30 minutes
                        print(f"Killing inactive socket for match {mid} (no data for >30 min)")
                        proc.terminate()
                        inactive_workers.append(mid)
                else:
                    # No log file means no data received - kill after 10 minutes
                    if hasattr(proc, 'start_time'):
                        if current_time - proc.start_time > 600:  # 10 minutes
                            print(f"Killing socket for match {mid} (no log file created)")
                            proc.terminate()
                            inactive_workers.append(mid)
                    else:
                        proc.start_time = current_time
        
        # Clean up inactive workers
        for mid in inactive_workers:
            temp_file = os.path.join("temp_sockets", f"fotmob_socket_{mid}.py")
            if temp_file in temp_files:
                try:
                    os.remove(temp_file)
                    temp_files.remove(temp_file)
                    print(f"Cleaned up temp file for inactive worker: {temp_file}")
                except Exception as e:
                    print(f"Error cleaning up {temp_file}: {e}")
            if mid in running_workers:
                del running_workers[mid]

        # Write back the updated DataFrame (only if not already written during UUID extraction)
        df.to_csv(FOTMOB_MATCHES_FILE, sep="|", index=False)

        # Calculate sleep time until next match starts
        next_match_time = None
        upcoming_matches = []
        
        for idx, row in today_matches.iterrows():
            if row['HasEnded'] == '1':
                continue
            ko_dt = parse_ko_time(row['KickOffTime'], row['MatchDate'])
            if ko_dt and ko_dt > now:
                upcoming_matches.append(ko_dt)
        
        if upcoming_matches:
            next_match_time = min(upcoming_matches)
            time_until_next = int((next_match_time - now).total_seconds())
            # Sleep until 5 minutes before next match, but at least 30 seconds
            sleep_seconds = max(30, time_until_next - 300)  # 300 = 5 minutes buffer
            next_match_str = next_match_time.strftime("%H:%M")
            if sleep_seconds > 300:
                print(f"Completed processing cycle. Next match at {next_match_str}, sleeping for {sleep_seconds} seconds ({sleep_seconds//60:.1f} minutes)...")
            else:
                print(f"Completed processing cycle. Next match at {next_match_str}, sleeping for {sleep_seconds} seconds...")
        else:
            sleep_seconds = POLL_INTERVAL_SECONDS
            print(f"Completed processing cycle. No upcoming matches today, sleeping for {sleep_seconds} seconds...")
        
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    try:
        # Clean up any existing temp files on startup
        cleanup_temp_files()
        main()
    except KeyboardInterrupt:
        print("\nShutting down runner...")
        # Clean up temp files on exit
        cleanup_temp_files()
        # Kill any stray socket processes older than 2 hours
        kill_old_socket_processes(max_age_seconds=2*3600)
    except Exception as e:
        print(f"Fatal error: {e}")
        cleanup_temp_files()