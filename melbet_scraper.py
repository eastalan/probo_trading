import requests
import json
import time
import re
import os
from datetime import datetime
import logging

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/melbet_scraper.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def setup_event_data_directory():
    """Create event_data directory if it doesn't exist"""
    os.makedirs('event_data', exist_ok=True)
    return 'event_data/melbet_events.psv'

# Removed Selenium-related functions since we're focusing on API-based event discovery

def scrape_1x2_api():
    """Scrape match data from the 1X2 API endpoint"""
    logger = logging.getLogger(__name__)
    matches = []
    
    # The 1X2 API endpoint that returns live football matches with team names
    api_url = "https://melbet-india.net/service-api/LiveFeed/Get1x2_VZip?sports=1&count=40&lng=en&gr=1182&mode=4&country=71&partner=8&getEmpty=true&virtualSports=true&noFilterBlockEvent=true"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://melbet-india.net/en/live/football'
    }
    
    try:
        logger.info(f"Fetching live matches from 1X2 API...")
        response = requests.get(api_url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully got 1X2 data")
            
            # Extract match data from the JSON response
            matches = extract_matches_from_1x2_response(data, logger)
            
        else:
            logger.error(f"Failed to get 1X2 data. Status code: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Error fetching 1X2 data: {e}")
    
    return matches

def extract_matches_from_1x2_response(data, logger):
    """Extract match data from 1X2 API response"""
    matches = []
    
    # Define leagues we're interested in
    target_leagues = [
        "England. Premier League",
        "Spain. La Liga",
        "Spain. LaLiga", 
        "Germany. Bundesliga",
        "Italy. Serie A",
        "France. Ligue 1",
        "Champions League",
        "Europa League",
        "USA. MLS"
    ]
    
    try:
        # Get all unique league names from the response
        if 'Value' in data and isinstance(data['Value'], list):
            all_leagues = set()
            for match in data['Value']:
                if 'L' in match and match['L']:
                    all_leagues.add(match['L'].strip())
            
            if all_leagues:
                logger.info("\n=== ACTIVE LEAGUES ===")
                for league in sorted(all_leagues):
                    logger.info(f"- {league}")
                logger.info("====================\n")
        
        # Process matches
        if 'Value' in data and isinstance(data['Value'], list):
            for match in data['Value']:
                try:
                    # Extract key information
                    match_id = match.get('I')  # Match ID
                    league = match.get('L', '').strip()  # League name
                    team1 = match.get('O1CT', '').strip() or match.get('O1', '').strip()  # Team 1 name
                    team2 = match.get('O2CT', '').strip() or match.get('O2', '').strip()  # Team 2 name
                    
                    # Skip if essential data is missing
                    if not all([match_id, team1, team2]):
                        continue
                    
                    # Filter by target leagues (case-insensitive partial match)
                    league_match = any(target.lower() in league.lower() for target in target_leagues)
                    
                    if league_match:
                        match_info = {
                            'id': str(match_id),
                            'team1': team1,
                            'team2': team2,
                            'league': league,
                            'filename': f"{team1.lower().replace(' ', '-')}-vs-{team2.lower().replace(' ', '-')}"
                        }
                        matches.append(match_info)
                        logger.info(f"Found match: {team1} vs {team2} ({league}) - ID: {match_id}")
                    
                except Exception as e:
                    logger.warning(f"Error processing match data: {e}")
                    continue
        
        logger.info(f"Extracted {len(matches)} matches from target leagues")
        
    except Exception as e:
        logger.error(f"Error parsing 1X2 response: {e}")
    
    return matches

def load_existing_events(psv_file):
    """Load existing events from PSV file to avoid duplicates"""
    existing_events = set()
    
    if os.path.exists(psv_file):
        try:
            with open(psv_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split('|')
                        if len(parts) >= 1:
                            event_id = parts[0].strip()
                            existing_events.add(event_id)
        except Exception as e:
            logging.getLogger(__name__).warning(f"Error reading existing events: {e}")
    
    return existing_events

def save_events_to_psv(matches, psv_file):
    """Save match events to PSV file, appending new ones and avoiding duplicates"""
    logger = logging.getLogger(__name__)
    
    if not matches:
        logger.warning("No matches found to save")
        return 0
    
    # Load existing events to avoid duplicates
    existing_events = load_existing_events(psv_file)
    
    # Filter out existing events
    new_matches = [match for match in matches if match['id'] not in existing_events]
    
    if not new_matches:
        logger.info(f"No new events found. All {len(matches)} events already exist in {psv_file}")
        return 0
    
    try:
        # Check if file exists to determine if we need to write header
        file_exists = os.path.exists(psv_file)
        
        with open(psv_file, 'a', encoding='utf-8') as f:
            # Write header if file is new
            if not file_exists:
                f.write("# Event_ID|Team1|Team2|League|Discovered_At|Filename\n")
            
            # Append new events
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for match in sorted(new_matches, key=lambda x: x['league']):
                line = f"{match['id']}|{match['team1']}|{match['team2']}|{match['league']}|{current_time}|{match['filename']}\n"
                f.write(line)
        
        logger.info(f"Appended {len(new_matches)} new events to {psv_file}")
        
        # Print summary
        print(f"\nEvent Discovery Summary:")
        print(f"  Total events found: {len(matches)}")
        print(f"  New events added: {len(new_matches)}")
        print(f"  Already existing: {len(matches) - len(new_matches)}")
        
        if new_matches:
            print(f"\nNew Events Added:")
            for match in sorted(new_matches, key=lambda x: x['league']):
                print(f"  {match['id']}: {match['team1']} vs {match['team2']} ({match['league']})")
        
        return len(new_matches)
            
    except Exception as e:
        logger.error(f"Failed to save events to PSV: {e}")
        return 0

def main():
    """Main function to discover and save match events"""
    logger = setup_logging()
    logger.info("Starting Melbet event discovery...")
    
    # Setup event data directory and file
    psv_file = setup_event_data_directory()
    logger.info(f"Event data will be saved to: {psv_file}")
    
    # Discover live matches from 1X2 API
    logger.info("Discovering live matches from 1X2 API...")
    matches = scrape_1x2_api()
    
    if matches:
        # Save events to PSV file
        new_events_count = save_events_to_psv(matches, psv_file)
        
        if new_events_count > 0:
            print(f"\nSuccessfully discovered and saved {new_events_count} new events to {psv_file}")
        else:
            print(f"\nNo new events to add. All discovered events already exist in {psv_file}")
            
    else:
        logger.warning("No matches found from target leagues!")
        
        # Fallback: try to get any live football matches
        logger.info("Trying fallback method...")
        fallback_matches = scrape_1x2_api_all_leagues()
        if fallback_matches:
            new_events_count = save_events_to_psv(fallback_matches, psv_file)
            if new_events_count > 0:
                print(f"\nFallback: Discovered and saved {new_events_count} new events")
        else:
            logger.error("No live matches found at all!")
    
    logger.info("Event discovery completed.")

def scrape_1x2_api_all_leagues():
    """Fallback: Get all live football matches without league filtering"""
    logger = logging.getLogger(__name__)
    matches = []
    
    api_url = "https://melbet-india.net/service-api/LiveFeed/Get1x2_VZip?sports=1&count=40&lng=en&gr=1182&mode=4&country=71&partner=8&getEmpty=true&virtualSports=true&noFilterBlockEvent=true"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://melbet-india.net/en/live/football'
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'Value' in data and isinstance(data['Value'], list):
                for match in data['Value'][:10]:  # Limit to first 10 matches
                    try:
                        match_id = match.get('I')
                        league = match.get('L', '').strip()
                        team1 = match.get('O1CT', '').strip()
                        team2 = match.get('O2CT', '').strip()
                        
                        if all([match_id, team1, team2]):
                            match_info = {
                                'id': str(match_id),
                                'team1': team1,
                                'team2': team2,
                                'league': league,
                                'filename': f"{team1.lower().replace(' ', '-')}-vs-{team2.lower().replace(' ', '-')}"
                            }
                            matches.append(match_info)
                            logger.info(f"Fallback match: {team1} vs {team2} ({league})")
                    except:
                        continue
                        
    except Exception as e:
        logger.error(f"Fallback method failed: {e}")
    
    return matches

if __name__ == "__main__":
    main()
