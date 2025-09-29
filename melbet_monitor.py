import requests
import json
import time
import os
import re
import yaml
from datetime import datetime
import logging
from utils.core.log_utils import get_dated_log_path

def sanitize_filename(filename):
    """Sanitize filename by removing/replacing invalid characters"""
    # Replace invalid characters with underscores
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Replace forward slashes specifically
    filename = filename.replace('/', '_')
    # Remove any remaining problematic characters
    filename = re.sub(r'[^\w\-_.]', '_', filename)
    # Remove multiple consecutive underscores
    filename = re.sub(r'_+', '_', filename)
    # Remove leading/trailing underscores
    filename = filename.strip('_')
    return filename

def setup_logging():
    """Setup logging configuration"""
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/melbet_monitor.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_events_from_psv(psv_file='event_data/melbet_events.psv'):
    """Load existing events from PSV file"""
    events = {}
    
    if os.path.exists(psv_file):
        try:
            with open(psv_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split('|')
                        if len(parts) >= 6:
                            event = {
                                'id': parts[0].strip(),
                                'team1': parts[1].strip(),
                                'team2': parts[2].strip(),
                                'league': parts[3].strip(),
                                'discovered_at': parts[4].strip(),
                                'filename': parts[5].strip()
                            }
                            events[event['id']] = event
        except Exception as e:
            logging.getLogger(__name__).error(f"Error loading events from PSV: {e}")
    
    return events

def load_config():
    """Load configuration from config.yaml"""
    try:
        with open('config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.getLogger(__name__).error(f"Error loading config: {e}")
        return {}

def discover_live_matches():
    """Discover current live matches from 1X2 API"""
    logger = logging.getLogger(__name__)
    
    # Load target leagues from config file
    config = load_config()
    target_leagues = config.get('melbet', {}).get('target_leagues', [])
    
    data = fetch_1x2_data()
    matches = {}
    
    if data and data.get('Success') and 'Value' in data:
        for match in data['Value']:
            try:
                match_id = str(match.get('I', ''))
                
                # Get league data
                league = match.get('LE', '') or match.get('L', '')
                
                # Get team names and scores
                team1 = match.get('O1E', match.get('O1', ''))
                team2 = match.get('O2E', match.get('O2', ''))
                
                # Get scores from SC.FS if available, otherwise from root level
                sc = match.get('SC', {})
                fs = sc.get('FS', {})
                score1 = fs.get('S1', 0)
                score2 = fs.get('S2', 0)
                
                # Store original team names for filename
                team1_original = team1
                team2_original = team2
                
                # Format team names with scores if match has started (for display only)
                cp = sc.get('CP', 0)
                if sc and cp and cp > 0:  # CP > 0 means match has started
                    team1_display = f"{team1} {score1}"
                    team2_display = f"{team2} {score2}"
                else:
                    team1_display = team1
                    team2_display = team2
                
                # Get odds from E array in old format (T1_C, T2_C, etc.)
                odds = {}
                for i, event in enumerate(match.get('E', []), 1):
                    coefficient = event.get('C', 0)
                    if coefficient:
                        odds[f'T{i}_C'] = coefficient

                if not all([match_id, team1, team2]):
                    continue
                
                # Filter by target leagues - EXACT MATCHES ONLY
                league_l = league.lower().strip()
                league_match = any(target.lower() == league_l for target in target_leagues)
                
                if league_match:
                    match_info = {
                        'id': str(match_id),
                        'team1': team1_display,
                        'team2': team2_display,
                        'league': league,
                        'discovered_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'filename': sanitize_filename(f"{team1_original.lower().replace(' ', '-')}-vs-{team2_original.lower().replace(' ', '-')}"),
                        'score1': score1,
                        'score2': score2,
                        'odds': odds,
                        'match_time': sc.get('SLS', 'Not started')
                    }
                    matches[str(match_id)] = match_info
                    
            except Exception as e:
                logger.warning(f"Error processing match: {e}")
                continue
    
    return matches

def update_psv_with_new_matches(existing_events, live_matches, psv_file='event_data/melbet_events.psv'):
    """Update PSV file with newly discovered matches"""
    logger = logging.getLogger(__name__)
    
    # Find new matches not in existing events
    new_matches = {}
    for match_id, match_info in live_matches.items():
        if match_id not in existing_events:
            new_matches[match_id] = match_info
    
    if new_matches:
        # Ensure directory exists
        os.makedirs('event_data', exist_ok=True)
        
        # Check if file exists to determine if we need header
        file_exists = os.path.exists(psv_file)
        
        try:
            with open(psv_file, 'a', encoding='utf-8') as f:
                if not file_exists:
                    f.write("# Event_ID|Team1|Team2|League|Discovered_At|Filename\n")
                
                for match in sorted(new_matches.values(), key=lambda x: x['league']):
                    # Extract original team names from filename for PSV (without scores)
                    filename_parts = match['filename'].split('-vs-')
                    if len(filename_parts) == 2:
                        team1_clean = filename_parts[0].replace('-', ' ').title()
                        team2_clean = filename_parts[1].replace('-', ' ').title()
                    else:
                        # Fallback to original team names without scores
                        team1_clean = match['team1'].split(' ')[0] if ' ' in match['team1'] else match['team1']
                        team2_clean = match['team2'].split(' ')[0] if ' ' in match['team2'] else match['team2']
                    
                    line = f"{match['id']}|{team1_clean}|{team2_clean}|{match['league']}|{match['discovered_at']}|{match['filename']}\n"
                    f.write(line)
            
            logger.info(f"Added {len(new_matches)} new matches to PSV file")
            for match in new_matches.values():
                logger.info(f"  New: {match['team1']} vs {match['team2']} ({match['league']}) - ID: {match['id']}")
                
        except Exception as e:
            logger.error(f"Error updating PSV file: {e}")
    
    return new_matches

def fetch_1x2_data():
    """Fetch live match data from 1X2 API"""
    logger = logging.getLogger(__name__)
    
    api_url = "https://melbet-596650.top/service-api/LiveFeed/Get1x2_VZip"
    params = {
        'sports': '1',
        'count': '40',
        'lng': 'en',
        'gr': '1182',
        'mode': '4',
        'country': '71',
        'partner': '8',
        'getEmpty': 'true',
        'virtualSports': 'true',
        'noFilterBlockEvent': 'true'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://melbet-596650.top/en/live/football'
    }
    
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=15)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch 1X2 data. Status code: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching 1X2 data: {e}")
        return None

def extract_c_values_for_match(data, match_id):
    """Extract C values with blocked status indicators from 1X2 API response"""
    logger = logging.getLogger(__name__)
    
    if not data or not data.get('Success') or 'Value' not in data:
        return {}
        
    try:
        for match in data['Value']:
            if str(match.get('I', '')) == str(match_id):
                # Extract odds with blocked indicators
                c_values = {}
                for event in match.get('E', []):
                    coefficient = event.get('C', 0)
                    t_value = event.get('T', 0)  # Get actual T value from API
                    if coefficient and t_value:
                        # Check if bet is blocked (B: true)
                        is_blocked = event.get('B', False)
                        if is_blocked:
                            c_values[f'T{t_value}_C'] = f"{coefficient}" + "-L"
                        else:
                            c_values[f'T{t_value}_C'] = coefficient

                return c_values
                    
    except Exception as e:
        logger.warning(f"Error extracting C values for match {match_id}: {e}")
    
    return {}

def log_c_values(match_info, c_values, previous_c_values):
    """Log C values with lock indicators for blocked bets"""
    logger = logging.getLogger(__name__)
    
    try:
        # Create log filename based on team names
        log_filename = f"{match_info['filename']}.log"
        log_path = get_dated_log_path('melbet_data', log_filename)
        
        # Only log if C values exist and have changed
        if c_values and (c_values != previous_c_values or not previous_c_values):
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] {match_info['team1']} vs {match_info['team2']} - C Values: {json.dumps(c_values)}\n")
            
            logger.info(f"Logged C values for {match_info['team1']} vs {match_info['team2']}: {c_values}")
            return c_values
        
    except Exception as e:
        logger.error(f"Error logging C values for {match_info['filename']}: {e}")
    
    return previous_c_values

def monitor_matches_continuously(poll_interval=0.3, psv_file='event_data/melbet_events.psv'):
    """Continuously monitor matches with dynamic discovery and log C value changes"""
    logger = logging.getLogger(__name__)
    
    # Load existing events from PSV
    existing_events = load_events_from_psv(psv_file)
    previous_c_values = {}
    
    logger.info(f"Starting dynamic match monitoring...")
    logger.info(f"Polling interval: {poll_interval} seconds")
    logger.info(f"Loaded {len(existing_events)} existing events from PSV")
    
    try:
        cycle_count = 0
        while True:
            cycle_count += 1
            
            # Discover current live matches
            live_matches = discover_live_matches()
            
            if live_matches:
                # Update PSV with any new matches found
                new_matches = update_psv_with_new_matches(existing_events, live_matches, psv_file)
                
                # Update existing events with new discoveries
                existing_events.update(new_matches)
                
                # Initialize C values tracking for new matches
                for match_id in new_matches:
                    if match_id not in previous_c_values:
                        previous_c_values[match_id] = {}
                
                # Monitor all current live matches
                data = fetch_1x2_data()
                if data:
                    for match_id, event in live_matches.items():
                        # Extract C values with blocked indicators for this specific match
                        c_values = extract_c_values_for_match(data, match_id)
                        
                        # Initialize if not exists
                        if match_id not in previous_c_values:
                            previous_c_values[match_id] = {}
                        
                        # Log C values with lock indicators (always log, even if no odds)
                        previous_c_values[match_id] = log_c_values(
                            event, 
                            c_values, 
                            previous_c_values[match_id]
                        )
                
                # Print status every 100 cycles
                if cycle_count % 100 == 0:
                    logger.info(f"Monitoring {len(live_matches)} live matches (cycle {cycle_count})")
            
            else:
                logger.warning("No live matches found, retrying...")
            
            # Wait before next poll
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Unexpected error in monitoring loop: {e}")
        raise

def main():
    """Main function to start dynamic continuous monitoring"""
    logger = setup_logging()
    logger.info("Starting Melbet dynamic continuous monitor...")
    
    print("\n🔄 Dynamic Match Monitor")
    print("This monitor will:")
    print("  • Auto-discover live matches from target leagues")
    print("  • Add new matches to PSV file automatically")
    print("  • Monitor C value changes for all live matches")
    print("  • Work independently of existing PSV content")
    print("\nPress Ctrl+C to stop monitoring\n")
    
    # Start dynamic continuous monitoring
    try:
        monitor_matches_continuously(poll_interval=0.3)
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")

if __name__ == "__main__":
    main()
