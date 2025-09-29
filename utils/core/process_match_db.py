#!/usr/bin/env python3
"""
Database-based match processing functions for fotmob_runner.py
"""

import os
import subprocess
import time
import re
from utils.db.fotmob_db_manager import FotMobDBManager
from .runner_logger import runner_logger, log_function_entry_exit

@log_function_entry_exit(runner_logger)
def process_match_uuid_and_socket_db(match_data, running_workers, temp_files):
    """
    Database version of process_match_uuid_and_socket function.
    Extract UUID and spawn socket for a match using database operations.
    """
    match_id = match_data['match_id']
    match_link = match_data['match_link']
    
    try:
        # Check if UUID already exists
        event_uuid = match_data.get('uuid')
        
        if not event_uuid or str(event_uuid).strip() in ["", "nan", "NaN"]:
            # Extract UUID using subprocess
            runner_logger.info(f"Extracting UUID for match {match_id}...")
            
            # Use the fotmob_uuid_extractor.py program
            FOTMOB_UUID_EXTRACTOR = os.path.join(os.path.dirname(__file__), "fotmob_uuid_extractor.py")
            
            result = subprocess.run(
                ["python3", FOTMOB_UUID_EXTRACTOR, match_link, "--match-id", match_id],
                capture_output=True,
                text=True,
                timeout=660
            )
            
            if result.returncode == 0:
                # Parse the UUID from the output
                for line in result.stdout.split('\n'):
                    if "SUCCESS: EVENT_UUID =" in line:
                        event_uuid = line.split("=")[-1].strip()
                        break
                
                if event_uuid:
                    # Update UUID in database
                    with FotMobDBManager() as db:
                        if db.update_match_uuid(match_id, event_uuid):
                            runner_logger.info(f"Extracted and saved UUID for match {match_id}: {event_uuid}")
                        else:
                            runner_logger.error(f"Failed to save UUID for match {match_id}")
                else:
                    runner_logger.warning(f"Could not extract UUID from output for match {match_id}")
                    return (match_id, False, None)
            else:
                runner_logger.error(f"UUID extraction failed for match {match_id}: {result.stderr}")
                return (match_id, False, None)
        else:
            # Double-check that the UUID is not invalid
            if event_uuid in ["", "nan", "NaN"]:
                runner_logger.warning(f"Invalid UUID '{event_uuid}' for match {match_id}")
                return (match_id, False, None)
            runner_logger.info(f"Using existing UUID for match {match_id}: {event_uuid}")
        
        # Only spawn socket if we have a valid UUID and not already running
        if event_uuid and not already_running(match_id, running_workers):
            try:
                # Create temp directory for socket files
                temp_dir = os.path.join(os.path.dirname(__file__), "temp_sockets")
                os.makedirs(temp_dir, exist_ok=True)
                
                # Create a modified version of fotmob_socket.py for this match
                temp_socket_file = os.path.join(temp_dir, f"fotmob_socket_{match_id}.py")
                
                # Read the original fotmob_socket.py
                FOTMOB_SOCKET_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "..", "fotmob_socket.py")
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
                # The temp file is in utils/core/temp_sockets/, so we need to go up 4 levels to reach project root
                modified_content = modified_content.replace(
                    'from utils.core.log_utils import get_dated_log_path',
                    'import sys\nimport os\nsys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))\nfrom utils.core.log_utils import get_dated_log_path'
                )
                
                # Write the modified script to the temp directory
                with open(temp_socket_file, 'w') as f:
                    f.write(modified_content)
                
                temp_files.add(temp_socket_file)
                
                # Launch the modified socket script
                proc = subprocess.Popen(["python3", temp_socket_file])
                proc.start_time = time.time()  # Track start time for cleanup
                running_workers[match_id] = proc
                runner_logger.info(f"Launched fotmob_socket for match {match_id} with EVENT_UUID: {event_uuid}")
                
                # Update DownloadFlag to 1 to indicate processing started
                with FotMobDBManager() as db:
                    if db.update_download_flag(match_id, 1):
                        runner_logger.info(f"Updated DownloadFlag to 1 for match {match_id}")
                    else:
                        runner_logger.error(f"Failed to update DownloadFlag for match {match_id}")
                
                return (match_id, True, event_uuid)
                
            except Exception as e:
                runner_logger.error(f"Error spawning socket for match {match_id}: {e}")
                return (match_id, False, event_uuid)
        elif event_uuid:
            runner_logger.info(f"Socket already running for match {match_id}")
            return (match_id, True, event_uuid)
        else:
            runner_logger.warning(f"No valid UUID available for match {match_id}, cannot spawn socket")
            return (match_id, False, None)
            
    except Exception as e:
        runner_logger.error(f"Error processing match {match_id}: {e}")
        return (match_id, False, None)

def already_running(match_id, running_workers):
    """Check if a match is already being processed"""
    return match_id in running_workers
