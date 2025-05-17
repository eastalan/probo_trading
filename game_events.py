import requests
import json
import os   # <-- Add this line
import time # For polling interval
import re # For parsing ShotID from log
from datetime import datetime

# --- Configuration ---
# IMPORTANT: The MATCH_ID_TO_FETCH and the headers (especially 'x-mas' and 'cookie')
# below MUST correspond to a valid session for that match.
# Using values based on user-provided cURL for matchId=4507113 and latest x-mas token.
MATCH_ID_TO_FETCH = 4507109
BASE_OUTPUT_DIR = "data"
EVENTS_SUBDIR = "match_events"
POLLING_INTERVAL_SECONDS = 0.001

# --- Global State ---
# This set will store the IDs of shots that have already been processed and logged.
# It will be populated from the log file on startup if the log exists.
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
        'cookie': 'NEXT_LOCALE=en-GB; _cc_id=2e905f48a8c1ed49d5f4ad058f7d91a5; panoramaId_expiry=1747584226438; panoramaId=1069694d6f0fc4890cc1c4c9b5224945a702b611b41825a830ac292215b37c44; panoramaIdType=panoIndiv; _gid=GA1.2.2016945245.1746979430; _hjSessionUser_2585474=eyJpZCI6ImIxODlkMGJkLWRlOWEtNTQyZi05NjE4LTNjN2M5ZGQ3NWVkMCIsImNyZWF0ZWQiOjE3NDY5Nzk0MjQzNjAsImV4aXN0aW5nIjp0cnVlfQ==; u:location=%7B%22countryCode%22%3A%22IN%22%2C%22regionId%22%3A%22MH%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22ccode3%22%3A%22IND%22%2C%22ccode3NoRegion%22%3A%22IND%22%2C%22timezone%22%3A%22Asia%2FCalcutta%22%7D; _gcl_au=1.1.1369954009.1747241326; cto_bundle=gnNyxl9IOEZlMXVuSU0lMkZTJTJGZFJtRTI0SFUwZmVmUjU1SlVmZU4wRVJJSE9yZ1Z6SFd5T0pzNm41SjRBWWk4MU9pUjhkdGtrJTJGUmxHWkolMkZrdVVVQU9lT2hHQ0dQQyUyQmZXUUM4VnR6Zno0aG5JamNlRDA3RVRmWjZWTDRSQUhuMkNwTDBybHh6JTJGVWUlMkJkeTVpMjYxTkJWZW5OblklMkZlcXlyVEVvJTJGeEtsbkk0YjJ0Uml6S0dHTmdTMSUyRkx4Z0pzSWhwMWtrTHA4NnhsVGcxUXdpTXpqZHBoa3RnYUtaOG9QTXV0VGU5RENYSFhFJTJCenJCN01IZXYxcXlINkh6TURDQ1IlMkJPOTd0Y09ocjVqZkY4cVBKbnBYaU4zTWFzWFFuR1VMQTB0bGNRSVd1SGlCdkd1eENqVjdtSWVtbGFWazFOTVNXaU1LZmEwMXBpTm12aVV2ZUFPaXMzM2F4SFNKTHdHQWVva0VsanloJTJGTHBnaG50aWVOMCUzRA; _hjSession_2585474=eyJpZCI6ImU1YzNmYmU0LWUyNTItNDNlMC1hOTQwLTQ1NWNiZjM3ODkzMiIsImMiOjE3NDcyNTMxNzgwNzYsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; __gads=ID=e4591c2b201e1d59:T=1746979425:RT=1747253178:S=ALNI_MaS1kr0EmFXchPBmOdCHVfwmiVvgw; __gpi=UID=000010bdf140af96:T=1746979425:RT=1747253178:S=ALNI_MaoF0crwtg7CPuzkhD9tKktVlSuKA; __eoi=ID=6c67024c3470435c:T=1746979425:RT=1747253178:S=AA-AfjYt_T8o6mjbsG-atkO-tFoi; _ga=GA1.1.1165890516.1746979424; _ga_G0V1WDW9B2=GS2.1.s1747253174$o14$g1$t1747253408$j24$l0$h2042514368; _ga_SQ24F7Q7YW=GS2.1.s1747253181$o10$g1$t1747253408$j0$l0$h0; _ga_K2ECMCJBFQ=GS2.1.s1747253181$o10$g1$t1747253408$j0$l0$h0; FCNEC=%5B%5B%22AKsRol-cCyZGIPd6PlykqPmOCeqIZFRqUg--m4kmnrLxyxSBZvmWIBy0YgqRPUslk5VxzjaHIARNkt40xy4_JB-LWyFA2f6TN6teRl5ffDK_OFHo4ueGZVUpkqpvb6WJeF2FC52YXpMJTqIDOaKNJ-KE0svgd_nC1w%3D%3D%22%5D%5D',
        'if-none-match': 'W/"174mixlphoc33lq"',
        'priority': 'u=1, i',
        'referer': f'https://www.fotmob.com/en-GB/match/{match_id}/playbyplay',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36',
        'x-mas': 'eyJib2R5Ijp7InVybCI6Ii9hcGkvdm90ZT9tYXRjaElkPTQ1MDcxMDkiLCJjb2RlIjoxNzQ3MzI5MzY2MDA4LCJmb28iOiJwcm9kdWN0aW9uOmJlMWJkZDg2N2ExMGRiMDJlMmIxMDA4MWFlNzRiNmUzODk5MzY4NWUtdW5kZWZpbmVkIn0sInNpZ25hdHVyZSI6IjBEMkMzMTQ0QkU4ODU2OUNEMjBCNUZBMjExMjA2RDVCIn0=='
    }
    response_obj = None # Initialize to handle early exceptions
    try:
        response_obj = requests.get(url, headers=headers, timeout=15)
        
        if response_obj.status_code == 304: 
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Server responded with 304 Not Modified. Content unchanged.")
            return None 

        response_obj.raise_for_status()  
        return response_obj.json() 
    except requests.exceptions.HTTPError as http_err:
        status_code_for_print = response_obj.status_code if response_obj is not None else 'N/A'
        print(f"HTTP error occurred: {http_err} - Status: {status_code_for_print}")
        if response_obj is not None and response_obj.status_code == 401:
            print("Received 401 Unauthorized. The 'x-mas' header and/or 'cookie' might be invalid or expired.")
    except requests.exceptions.Timeout:
        print(f"Request timed out while trying to fetch data for match ID {match_id}.")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    except json.JSONDecodeError:
        response_text_for_print = response_obj.text[:500] if response_obj is not None else 'No response object'
        print(f"Error: Failed to decode JSON response. Response text: {response_text_for_print}")
    except Exception as e:
        print(f"An unexpected error occurred during fetch: {e}")
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

def load_processed_ids_from_log(log_file_path):
    """
    Loads already processed shot IDs from the given log file to prevent re-logging.
    """
    loaded_ids = set()
    if not os.path.exists(log_file_path):
        print(f"Log file {log_file_path} not found. Starting with no processed IDs.")
        return loaded_ids
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Updated regex to match "ShotID: <id>"
                match = re.search(r"ShotID:\s*(\d+)", line)
                if match:
                    try:
                        shot_id = int(match.group(1))
                        loaded_ids.add(shot_id)
                    except ValueError:
                        print(f"Warning: Could not parse shot ID from log line: {line.strip()}")
        print(f"Loaded {len(loaded_ids)} processed shot IDs from {log_file_path}")
    except IOError as e:
        print(f"Error reading log file {log_file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while loading processed IDs from log: {e}")
    return loaded_ids

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

    # Load processed IDs from the log file *before* processing initial data
    processed_shot_ids = load_processed_ids_from_log(dynamic_output_file_path)

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

