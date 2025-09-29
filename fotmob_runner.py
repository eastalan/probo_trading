import os
import time
import datetime
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.core.process_utils import kill_old_socket_processes
from utils.db.fotmob_db_manager import FotMobDBManager
from utils.core.process_match_db import process_match_uuid_and_socket_db, already_running
from utils.core.runner_logger import runner_logger, log_function_entry_exit

POLL_INTERVAL_SECONDS = 30  # How often to check for new matches

# All match processing functions have been moved to utils/core/process_match_db.py

@log_function_entry_exit(runner_logger)
def cleanup_temp_files():
    """
    Clean up temporary socket files.
    """
    try:
        # Clean up files in main directory (legacy)
        for file in os.listdir('.'):
            if file.startswith('fotmob_socket_') and file.endswith('.py'):
                os.remove(file)
                runner_logger.info(f"Cleaned up temporary file: {file}")
        
        # Clean up files in temp_sockets directory
        temp_dir = "temp_sockets"
        if os.path.exists(temp_dir):
            for file in os.listdir(temp_dir):
                if file.startswith('fotmob_socket_') and file.endswith('.py'):
                    os.remove(os.path.join(temp_dir, file))
                    runner_logger.info(f"Cleaned up temporary file: {os.path.join(temp_dir, file)}")
            # Remove empty directory
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass  # Directory not empty, that's fine
    except Exception as e:
        runner_logger.error(f"Error cleaning up temp files: {e}")

# Global variables for signal handling
running_workers = {}  # match_id: subprocess.Popen
temp_files = set()  # Track temporary files for cleanup

@log_function_entry_exit(runner_logger)
def signal_handler(signum, frame):
    """Handle SIGINT (Ctrl+C) gracefully by terminating all subprocesses"""
    runner_logger.info("Received interrupt signal, shutting down gracefully...")
    
    # Terminate all running worker processes
    for match_id, proc in running_workers.items():
        try:
            runner_logger.info(f"Terminating worker for match {match_id}...")
            proc.terminate()
            # Give process 3 seconds to terminate gracefully
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                runner_logger.warning(f"Force killing worker for match {match_id}...")
                proc.kill()
        except Exception as e:
            runner_logger.error(f"Error terminating worker {match_id}: {e}")
    
    # Clean up temp files
    cleanup_temp_files()
    
    # Kill any remaining socket processes
    kill_old_socket_processes(max_age_seconds=0)  # Kill all socket processes
    
    runner_logger.info("Shutdown complete")
    sys.exit(0)

@log_function_entry_exit(runner_logger)
def main():
    global running_workers, temp_files
    
    runner_logger.info("Starting FotMob Runner main loop")
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        now = datetime.datetime.now()
        today_str = now.strftime("%Y%m%d")
        
        # Use the new ko_datetime column for time-based filtering
        # Get matches within the last 4 hours AND any unprocessed matches regardless of start time
        four_hours_ago = now - datetime.timedelta(hours=4)
        
        # Get all processable matches in the 4-hour window
        with FotMobDBManager() as db:
            all_processable_matches = db.get_processable_matches_in_time_window(four_hours_ago, now)
        
        # Get all matches for sleep calculation (including future matches)
        with FotMobDBManager() as db:
            all_matches = db.get_upcoming_matches(now)
        
        if not all_matches:
            runner_logger.info("No matches found")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        
        # Auto-expire matches and reset invalid UUIDs for both dates
        with FotMobDBManager() as db:
            total_expired = 0
            total_reset = 0
            total_expired += db.bulk_update_expired_matches(today_str, 3.5)
            total_reset += db.reset_invalid_uuids(today_str)
            
            if total_expired > 0:
                runner_logger.info(f"Auto-expired {total_expired} matches")
            if total_reset > 0:
                runner_logger.info(f"Reset {total_reset} matches with invalid UUIDs")
        
        # Process matches in parallel for UUID extraction and socket spawning
        # All matches from the query are already within the 4-hour window
        matches_to_process = all_processable_matches
        
        for match in matches_to_process:
            ko_dt = match.get('ko_datetime')
            if ko_dt:
                time_diff = (now - ko_dt).total_seconds() / 3600  # hours
                if time_diff > 0:
                    runner_logger.info(f"Match {match['match_id']} ({match['home_team']} vs {match['away_team']}) started {time_diff:.1f}h ago - processing")
                else:
                    runner_logger.info(f"Match {match['match_id']} ({match['home_team']} vs {match['away_team']}) starts in {abs(time_diff):.1f}h - processing")
            else:
                runner_logger.info(f"Match {match['match_id']} ({match['home_team']} vs {match['away_team']}) - no datetime - processing")
        
        if matches_to_process:
            runner_logger.info(f"Processing {len(matches_to_process)} started matches in parallel...")
            
            # Use ThreadPoolExecutor for parallel UUID extraction
            with ThreadPoolExecutor(max_workers=min(20, len(matches_to_process))) as executor:
                # Submit all matches for processing
                future_to_match = {
                    executor.submit(process_match_uuid_and_socket_db, match_data, running_workers, temp_files): match_data['match_id']
                    for match_data in matches_to_process
                }
                
                # Process completed futures as they finish
                for future in as_completed(future_to_match):
                    match_id = future_to_match[future]
                    try:
                        result_match_id, success, event_uuid = future.result()
                        if success:
                            runner_logger.info(f"Successfully processed match {result_match_id}")
                        else:
                            runner_logger.warning(f"Failed to process match {result_match_id}")
                    except Exception as e:
                        runner_logger.error(f"Exception processing match {match_id}: {e}")
            
            runner_logger.info("Completed parallel processing of matches")

        # Clean up finished workers and their temp files
        finished = [mid for mid, proc in running_workers.items() if proc.poll() is not None]
        for mid in finished:
            runner_logger.info(f"Worker for match {mid} finished.")
            # Clean up the temporary socket file for this match
            temp_file = os.path.join("temp_sockets", f"fotmob_socket_{mid}.py")
            if temp_file in temp_files:
                try:
                    os.remove(temp_file)
                    temp_files.remove(temp_file)
                    runner_logger.info(f"Cleaned up temp file: {temp_file}")
                except Exception as e:
                    runner_logger.error(f"Error cleaning up {temp_file}: {e}")
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
                        runner_logger.info(f"Killing inactive socket for match {mid} (no data for >30 min)")
                        proc.terminate()
                        inactive_workers.append(mid)
                else:
                    # No log file means no data received - kill after 10 minutes
                    if hasattr(proc, 'start_time'):
                        if current_time - proc.start_time > 600:  # 10 minutes
                            runner_logger.info(f"Killing socket for match {mid} (no log file created)")
                            proc.terminate()
                            inactive_workers.append(mid)
                    else:
                        proc.start_time = current_time
        
        # Clean up inactive workers
        for mid in inactive_workers:
            # Temp files are created in utils/core/temp_sockets/
            temp_file = os.path.join("utils", "core", "temp_sockets", f"fotmob_socket_{mid}.py")
            if temp_file in temp_files:
                try:
                    os.remove(temp_file)
                    temp_files.remove(temp_file)
                    runner_logger.info(f"Cleaned up temp file for inactive worker: {temp_file}")
                except Exception as e:
                    runner_logger.error(f"Error cleaning up {temp_file}: {e}")
            if mid in running_workers:
                del running_workers[mid]

        # Database updates are handled automatically in process functions

        # Calculate sleep time until next match starts
        # all_matches already contains upcoming matches sorted by ko_datetime
        if all_matches:
            next_match = all_matches[0]  # First match is the earliest
            next_match_time = next_match.get('ko_datetime')
            
            if next_match_time:
                time_until_next = (next_match_time - now).total_seconds()
                # Sleep until 5 minutes before next match, but at least 30 seconds
                sleep_seconds = max(30, time_until_next - 300)  # 300 = 5 minutes buffer
            else:
                sleep_seconds = 300  # Default 5 minutes if no ko_datetime
        else:
            sleep_seconds = 300  # Default 5 minutes if no upcoming matches
        
        if all_matches and next_match_time:
            runner_logger.info(f"Next match at {next_match_time.strftime('%H:%M')} - sleeping for {sleep_seconds/60:.1f} minutes")
        else:
            runner_logger.info("No upcoming matches found - sleeping for 5 minutes")
        
        time.sleep(sleep_seconds)

if __name__ == "__main__":
    try:
        runner_logger.info("Starting FotMob Runner application")
        # Clean up any existing temp files on startup
        cleanup_temp_files()
        main()
    except KeyboardInterrupt:
        # This should not be reached due to signal handler, but keep as backup
        signal_handler(signal.SIGINT, None)
    except Exception as e:
        runner_logger.error(f"Fatal error: {e}")
        cleanup_temp_files()
        # Terminate any running workers on fatal error
        for match_id, proc in running_workers.items():
            try:
                proc.terminate()
            except:
                pass