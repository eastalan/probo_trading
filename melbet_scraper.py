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
    """Scrape match data from the 1X2 API endpoint using authenticated session"""
    logger = logging.getLogger(__name__)
    matches = []
    
    # Use authenticated API endpoints from melbet_bet_placer.py
    api_urls = [
        "https://melbet-596650.top/service-api/LiveFeed/Get1x2_VZip?sports=1&count=40&lng=en&gr=1182&mode=4&country=71&partner=8&getEmpty=true&virtualSports=true&noFilterBlockEvent=true",
        "https://melbet-596650.top/service-api/LiveFeed/Get1x2?sports=1&count=40&lng=en&gr=1182&mode=4&country=71&partner=8&getEmpty=true&virtualSports=true&noFilterBlockEvent=true",
        "https://melbet-596650.top/service-api/LiveFeed/GetLive?sports=1&count=40&lng=en"
    ]
    
    # Use full authentication headers and cookies from melbet_bet_placer.py
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'content-type': 'application/json',
        'is-srv': 'false',
        'origin': 'https://melbet-596650.top',
        'priority': 'u=1, i',
        'referer': 'https://melbet-596650.top/en/live/football',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
        'x-app-n': '__BETTING_APP__',
        'x-auth': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjEiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiI1MC8xMzcwMDIyNDczIiwicGlkIjoiOCIsImp0aSI6IjAvYmJjMTdhYTljMzcwNWNkOTgxZGFlZjI5OTRiZTU2YmFhZmIzYzU1ZDc5ZDQwNTcwMjY4MzZiOGU2ZDQ3OWEwOCIsImFwcCI6Ik5BIiwiaW5uZXIiOiJ0cnVlIiwibmJmIjoxNzU2NTM5NDM0LCJleHAiOjE3NTY1NTM4MzQsImlhdCI6MTc1NjUzOTQzNH0.AoyQnW3VUV2GEfv_16ga7nvHf6nJbiBgK_ILV-rzxx7FVY6hFUWJazdKk5_lcA0iZiW-yPrPxPNM8AzoqvvQ8A',
        'x-hd': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZXYiOjEsImd1aWQiOiJXR0NRaUYxVVhtSzN6WFVJT0Nka1BRZ0RDRG5zN3pnaHBVU1diTTNNL3dpSERXMzF3Uy9DbEtMWDE5SGVnaVZvNEVpbkN6cmpzL0tHKzMzMWtkZE9nYjkyK1FQeFZaaUZ1TDZnUllsWUJWTUdqVVRtVG1peU02UXpxWXJSZUVTQkphU3g1YWVZQUFjbnRoR1JSZmhOQ2dNdHlHOUVtSHlRMFFqNlgrWXVSemd4ZXJLVUZNbnNwZXdGOWJjVmxWb0RxMkdzWEVRNVJUaWR3N2NiTlIzTFVQUnc5MjM4ZHRhbWFJSE4zRmlDV0JTc0xaRHA4dWJhbktSVzNIRHY3cTZzRTdac0cxME9VYk4wT3V1YURSWXA4U3cya2tCaitxektueEplMVdUTEhMUmxaM3FHblNBaWQzSjdYL0h6N3hyTnFSOE9RYlQvMC9MMDRWYzJGMW93S1BnRzR2dXZaWi9TVnVpeEMwSkpJc1prQjU5Mm84T2VYU09zN1lQUlB4eVM5cGpBbWIwenQ5OVdsLzNSZFNYbHRNR3NJa1lEUzVRNk8rMW44NkhpWGhETEZKUUJDb1F4cDZnNld4NDZNclhyNTRWRDR0R0s1MzFPTjZxQmRDREdYUkUreDhmUHMzMEcyTlBMM2g3cVJPemtlbWRCLzFJQ0J4THFIc2V5c0VXRnlOQzhzSzlIdVpZT2M5KzRESTM2Q2xxZkNPZmxHektSc3FLeDFMVWNJL1B0eW9GekZpTTU4b2JwUHhIRG15WVpEZFhUVDh2aXZEc2l0d21mS1ozRWd0alNnRVcvR3B3SHVUQXlrUWRaIiwiZXhwIjoxNzU2NTUzNzkzLCJpYXQiOjE3NTY1MzkzOTN9.szrl8t_MrdV7twIufyhm6QxkyZ791dohQCpG6SFIMH4YWHvjUJYlWTFPqzz8DyU3Vo3Ryb0RoXK-5VauoFaNPQ',
        'x-requested-with': 'XMLHttpRequest',
        'x-svc-source': '__BETTING_APP__'
    }
    
    cookies = {
        'platform_type': 'desktop',
        'gw-blk': 'eyJkYXRhIjp7ImlkIjowLCJkaXNwbGF5VHlwZUlkIjowLCJ0ZW1wbGF0ZVR5cGVJZCI6MCwidGVtcGxhdGVJZCI6MH0sImJyZWFkY3J1bWJzIjpudWxsfQ==',
        'lng': 'en',
        'cookies_agree_type': '3',
        'tzo': '5.5',
        'is12h': '0',
        'auid': 'Z29zZGivaf999nlCCVd6Ag==',
        'che_g': '1a93e6c4-8bfe-432b-91e2-5d526bd73450',
        'sh.session.id': '6af617fc-90be-4334-a3f3-be8695273c79',
        'application_locale': 'en',
        'x-banner-api': '',
        '_gcl_au': '1.1.127769253.1756326412',
        'sbjs_migrations': '1418474375998%3D1',
        'sbjs_current_add': 'fd%3D2025-08-28%2001%3A56%3A51%7C%7C%7Cep%3Dhttps%3A%2F%2Fmelbet-596650.top%2Fen%2Flive%2Ffootball%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F',
        'sbjs_first_add': 'fd%3D2025-08-28%2001%3A56%3A51%7C%7C%7Cep%3Dhttps%3A%2F%2Fmelbet-596650.top%2Fen%2Flive%2Ffootball%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F',
        'sbjs_current': 'typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29',
        'sbjs_first': 'typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29',
        '_gid': 'GA1.2.1412766403.1756502650',
        '_glhf': '1756557167',
        'ggru': '160',
        'ua': '1370022473',
        'uhash': '610d4b4b2df94a50aabb037a641d4c88',
        'cur': 'INR',
        'user_token': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjEiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiI1MC8xMzcwMDIyNDczIiwicGlkIjoiOCIsImp0aSI6IjAvYmJjMTdhYTljMzcwNWNkOTgxZGFlZjI5OTRiZTU2YmFhZmIzYzU1ZDc5ZDQwNTcwMjY4MzZiOGU2ZDQ3OWEwOCIsImFwcCI6Ik5BIiwiaW5uZXIiOiJ0cnVlIiwibmJmIjoxNzU2NTM5NDM0LCJleHAiOjE3NTY1NTM4MzQsImlhdCI6MTc1NjUzOTQzNH0.AoyQnW3VUV2GEfv_16ga7nvHf6nJbiBgK_ILV-rzxx7FVY6hFUWJazdKk5_lcA0iZiW-yPrPxPNM8AzoqvvQ8A',
        'newuser_review': '928914561',
        'reg_id': 'ad25e049bef49265c4b8b6e396bc2279',
        'firstAuthRedirect': '1',
        'SESSION': '97883c1b7a1dd8a40fed7b3de7a37ae8',
        'post_reg_type': 'phone_reg',
        'PAY_SESSION': '20e5c44e447d0d0b153de9a6f45bb9b6',
        'sbjs_udata': 'vst%3D4%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Linux%3B%20Android%206.0%3B%20Nexus%205%20Build%2FMRA58N%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F139.0.0.0%20Mobile%20Safari%2F537.36',
        'sbjs_session': 'pgs%3D3%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fmelbet-596650.top%2Fen%2Flive%2Ffootball',
        '_ga': 'GA1.2.98088144.1756326412',
        '_gat_UA-244626893-1': '1',
        'window_width': '842',
        '_ga_435XWQE678': 'GS2.1.s1756539376$o4$g1$t1756540355$j29$l0$h0',
        '_ga_8SZ536WC7F': 'GS2.1.s1756539376$o4$g1$t1756540355$j19$l1$h73569795'
    }
    
    for api_url in api_urls:
        try:
            logger.info(f"Trying API endpoint: {api_url}")
            response = requests.get(api_url, headers=headers, cookies=cookies, timeout=15)
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Content-Type: {response.headers.get('content-type', 'Unknown')}")
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                
                if 'application/json' in content_type or 'text/plain' in content_type:
                    if not response.text.strip():
                        logger.error("Empty response received from API")
                        continue
                        
                    try:
                        data = response.json()
                        logger.info(f"Successfully got JSON data from {api_url}")
                        
                        # Extract match data from the JSON response
                        matches = extract_matches_from_1x2_response(data, logger)
                        if matches:
                            logger.info(f"Found {len(matches)} matches from API")
                            break
                        else:
                            logger.info("No matches found in response, trying next endpoint")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}")
                        logger.info(f"Response preview: {response.text[:200]}")
                        continue
                        
                else:
                    logger.warning(f"Unexpected content type: {content_type}")
                    logger.info(f"Response preview: {response.text[:200]}")
                    continue
                    
            else:
                logger.error(f"HTTP {response.status_code} from {api_url}")
                continue
                
        except Exception as e:
            logger.error(f"Error with {api_url}: {e}")
            continue
    
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
        "USA. MLS",
        "Japan. J-League",
        "Argentina. Primera Division",
        "Turkey. SuperLiga",
        "Austria. Bundesliga",
        "Netherlands. Eredivisie",
        "Portugal. Primeira Liga",
        "Brazil. Campeonato Brasileiro. Serie A",
        "Russia. Premier League",
        "Scotland. Premier League",
        "China. Super League",
        "Denmark. Superliga",
        "Sweden. Allsvenskan",
        "Norway. Eliteserien",
        "World Cup 2026 Qualification. Europe",
        "Friendlies. National Teams",
        "Germany. Bundesliga. Women",
        "Spain. Segunda Division",
        "World Cup 2026 Qualification. CONCACAF",
        "World Cup 2026. Qualification. South America",
        "Premier League International Cup"
  
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
