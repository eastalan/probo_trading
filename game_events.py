import requests
import json
import os
import time
import re
from datetime import datetime

# --- Configuration ---
MATCH_ID_TO_FETCH = 4691209
BASE_OUTPUT_DIR = "data"
EVENTS_SUBDIR = "match_events"
POLLING_INTERVAL_SECONDS = 0.001

# --- Global State ---
processed_shot_ids = set()

# --- Helper Functions ---

def ensure_dir_exists(directory_path):
    """Ensures that the specified directory exists, creating it if necessary."""
    try:
        os.makedirs(directory_path, exist_ok=True)
        print(f"Ensured directory exists: {directory_path}")
    except OSError as e:
        print(f"Error creating directory {directory_path}: {e}")
        raise

def fetch_match_data(match_id):
    """
    Fetches full match details from FotMob API using the comprehensive headers.

    Args:
        match_id (int or str): The ID of the match.

    Returns:
        dict: The full JSON response data if successful, otherwise None.
    """
    url = f"https://www.fotmob.com/api/matchDetails?matchId={match_id}"
    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'referer': f'https://www.fotmob.com/en-GB/match/{match_id}/playbyplay',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36',
    }
    try:
        response_obj = requests.get(url, headers=headers, timeout=15)
        response_obj.raise_for_status()
        return response_obj.json()
    except Exception as e:
        print(f"Error fetching match data: {e}")
        return None

def get_team_names_for_filename(general_info):
    if not general_info or not isinstance(general_info, dict):
        print("Error: 'general' info is missing or not a dictionary. Cannot determine team names for filename.")
        return "UnknownHomeTeam", "UnknownAwayTeam"
    home_team_name = general_info.get('homeTeam', {}).get('name', 'UnknownHomeTeam')
    away_team_name = general_info.get('awayTeam', {}).get('name', 'UnknownAwayTeam')
    home_team_name = "".join(c if c.isalnum() else "_" for c in home_team_name)
    away_team_name = "".join(c if c.isalnum() else "_" for c in away_team_name)
    return home_team_name, away_team_name

def extract_team_mapping(match_data):
    team_mapping = {}
    if not match_data or 'general' not in match_data:
        return team_mapping 
    general_info = match_data['general']
    if 'homeTeam' in general_info and isinstance(general_info['homeTeam'], dict):
        home_team_id = general_info['homeTeam'].get('id')
        home_team_name = general_info['homeTeam'].get('name')
        if home_team_id and home_team_name:
            team_mapping[home_team_id] = home_team_name
    if 'awayTeam' in general_info and isinstance(general_info['awayTeam'], dict):
        away_team_id = general_info['awayTeam'].get('id')
        away_team_name = general_info['awayTeam'].get('name')
        if away_team_id and away_team_name:
            team_mapping[away_team_id] = away_team_name
    return team_mapping

def process_and_log_shots(match_data, team_mapping, output_file_path, shot_ids_already_processed):
    """
    Processes shotmap events, logs new ones to console and a file.
    Includes ShotID in the log entry.
    """
    if not match_data or 'content' not in match_data or \
       'shotmap' not in match_data['content'] or \
       'shots' not in match_data['content']['shotmap']:
        return shot_ids_already_processed

    shots = match_data['content']['shotmap'].get('shots', [])
    new_events_logged_this_poll = 0

    if not shots:
        return shot_ids_already_processed

    try:
        with open(output_file_path, 'a', encoding='utf-8') as f:
            for shot in shots:
                shot_id = shot.get('id')
                if shot_id is None: 
                    continue
                
                if shot_id in shot_ids_already_processed:
                    continue 

                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") 
                event_type = shot.get('eventType', 'N/A')
                team_id = shot.get('teamId')
                player_name = shot.get('playerName', 'N/A')
                situation = shot.get('situation', 'N/A')
                shot_type = shot.get('shotType', 'N/A')
                expected_goals = shot.get('expectedGoals', 0.0)
                minute = shot.get('min', 'N/A')

                team_name = team_mapping.get(team_id, f"Unknown Team ({team_id})")

                # Updated log entry to include ShotID
                log_entry = (
                    f"[{timestamp}] ShotID: {shot_id}, Min: {minute}', Event: {event_type}, Team: {team_name}, Player: {player_name}, "
                    f"Situation: {situation}, Shot Type: {shot_type}, xG: {expected_goals:.4f}"
                )

                print(log_entry) 
                f.write(log_entry + "\n") 

                if event_type == "Goal":
                    goal_alert_message = (
                        f"🚨 GOAL! 🚨 [{timestamp}] ShotID: {shot_id}, Min: {minute}', Team: {team_name}, Player: {player_name}, "
                        f"Situation: {situation}, Shot Type: {shot_type}, xG: {expected_goals:.4f}"
                    )
                    print(goal_alert_message)
                    print('\a') 
                    f.write(goal_alert_message + " (GOAL ALERT)\n")

                shot_ids_already_processed.add(shot_id) 
                new_events_logged_this_poll += 1
        
        if new_events_logged_this_poll > 0:
            print(f"Logged {new_events_logged_this_poll} new event(s) to {output_file_path}")

    except IOError as e:
        print(f"Error writing to file {output_file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during shot processing: {e}")
    
    return shot_ids_already_processed

# --- Main Execution ---
if __name__ == "__main__":
    print(f"FotMob Live Event Logger - Polling for Match ID: {MATCH_ID_TO_FETCH}")
    print(f"Using comprehensive headers with latest x-mas token for matchId={MATCH_ID_TO_FETCH}.")
    print(f"Polling interval: {POLLING_INTERVAL_SECONDS} seconds.")
    print("Attempting to load previously logged event IDs to prevent duplicates on restart.")

    target_output_dir = os.path.join(BASE_OUTPUT_DIR, EVENTS_SUBDIR)
    try:
        ensure_dir_exists(BASE_OUTPUT_DIR)
        ensure_dir_exists(target_output_dir)
    except Exception:
        print(f"Fatal: Could not create required output directories ({target_output_dir}). Exiting.")
        exit(1)

    print("Performing initial data fetch to determine team names for log file...")
    initial_match_data = fetch_match_data(MATCH_ID_TO_FETCH)

    if not initial_match_data:
        print("Fatal: Could not fetch initial match data. Cannot determine log filename or proceed. Exiting.")
        print("This might be due to an incorrect/expired 'x-mas' header, 'cookie', or network issues.")
        exit(1)
    
    general_info_for_filename = initial_match_data.get('general')
    if not general_info_for_filename:
        print("Fatal: 'general' section missing in initial data. Cannot determine team names for log filename. Exiting.")
        exit(1)

    home_team_name, away_team_name = get_team_names_for_filename(general_info_for_filename)
    
    log_filename = f"{home_team_name}_vs_{away_team_name}_{MATCH_ID_TO_FETCH}.log"
    dynamic_output_file_path = os.path.join(target_output_dir, log_filename)
    print(f"Logging events to: {dynamic_output_file_path}")

    current_team_name_map = extract_team_mapping(initial_match_data)
    if not current_team_name_map:
        print("Warning: Could not extract initial team ID to name mapping. Team names in logs might be 'Unknown'.")

    print("Processing any existing shots from initial fetch (will skip already logged IDs)...")
    # The global processed_shot_ids set is passed and updated
    processed_shot_ids = process_and_log_shots(
        initial_match_data,
        current_team_name_map,
        dynamic_output_file_path,
        processed_shot_ids 
    )
    print(f"Initial processing complete. Total known processed IDs: {len(processed_shot_ids)}.")
    print(f"\n--- Starting continuous polling for new events (Ctrl+C to stop) ---")

    try:
        while True:
            current_match_data = fetch_match_data(MATCH_ID_TO_FETCH)

            if current_match_data: 
                updated_team_map = extract_team_mapping(current_match_data)
                if updated_team_map: 
                    current_team_name_map = updated_team_map
                elif not current_team_name_map and not updated_team_map:
                     print("Warning: Team mapping is currently unavailable.")

                processed_shot_ids = process_and_log_shots(
                    current_match_data,
                    current_team_name_map, 
                    dynamic_output_file_path,
                    processed_shot_ids 
                )
            else: 
                  # fetch_match_data already prints messages for errors or 304
                  print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No new data from fetch or fetch error. Will retry polling.")
            
            time.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nPolling stopped by user (Ctrl+C). Exiting.")
    except Exception as e:
        print(f"An unexpected error occurred in the main polling loop: {e}")
    finally:
        print("Script finished.")

