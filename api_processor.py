#!/usr/bin/env python3
"""
1xBetHind API Data Processor
Fetches and processes live sports data from 1xbethind.com API
"""

import requests
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import os

class BettingAPIProcessor:
    """Process live betting data from 1xbethind API"""
    
    def __init__(self, log_level=logging.INFO):
        self.setup_logging(log_level)
        self.logger = logging.getLogger(__name__)
        
        # API configuration
        self.api_url = "https://1xbethind.com/service-api/LiveFeed/Get1x2_VZip"
        self.params = {
            'sports': '1',
            'count': '40', 
            'lng': 'en',
            'gr': '413',
            'mode': '4',
            'country': '71',
            'partner': '71',
            'getEmpty': 'true',
            'virtualSports': 'true',
            'noFilterBlockEvent': 'true'
        }
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://1xbethind.com/en/live/football',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        # Target leagues to monitor
        self.target_leagues = [
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
    
    def setup_logging(self, level):
        """Setup logging configuration"""
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/api_processor.log'),
                logging.StreamHandler()
            ]
        )
    
    def fetch_api_data(self) -> Optional[List[Dict]]:
        """Fetch data from 1xbethind API"""
        try:
            response = requests.get(
                self.api_url, 
                headers=self.headers, 
                params=self.params, 
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"Successfully fetched {len(data) if isinstance(data, list) else 0} matches")
                return data
            else:
                self.logger.error(f"API request failed with status code: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching API data: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching API data: {e}")
            return None
    
    def extract_match_info(self, match: Dict) -> Optional[Dict]:
        """Extract relevant information from a single match"""
        try:
            match_id = str(match.get('I', ''))
            if not match_id:
                return None
            
            # Get league data
            league = match.get('LE', '') or match.get('L', '')
            
            # Get team names
            team1 = match.get('O1E', match.get('O1', ''))
            team2 = match.get('O2E', match.get('O2', ''))
            
            if not all([team1, team2]):
                return None
            
            # Get match status and scores
            sc = match.get('SC', {})
            fs = sc.get('FS', {})
            score1 = fs.get('S1', 0)
            score2 = fs.get('S2', 0)
            match_status = sc.get('SLS', 'Not started')
            match_phase = sc.get('CP', 0)  # 0=not started, 1=1st half, 2=2nd half, etc.
            
            # Get odds (1X2 market)
            odds = self.extract_odds(match)
            
            # Get additional match info
            country = match.get('CN', '')
            start_time = match.get('S', 0)
            location = match.get('MIO', {}).get('Loc', '')
            
            match_info = {
                'id': match_id,
                'league': league,
                'country': country,
                'team1': team1,
                'team2': team2,
                'score1': score1,
                'score2': score2,
                'match_status': match_status,
                'match_phase': match_phase,
                'odds': odds,
                'start_time': datetime.fromtimestamp(start_time) if start_time else None,
                'location': location,
                'fetched_at': datetime.now()
            }
            
            return match_info
            
        except Exception as e:
            self.logger.warning(f"Error extracting match info: {e}")
            return None
    
    def extract_odds(self, match: Dict) -> Dict:
        """Extract 1X2 odds from match data"""
        odds = {}
        
        try:
            for event in match.get('AE', []):
                if event.get('G') == 1:  # 1X2 market
                    for me in event.get('ME', []):
                        bet_type = me.get('T')
                        coefficient = me.get('C', 0)
                        
                        if bet_type == 1:  # Home win
                            odds['1'] = coefficient
                        elif bet_type == 2:  # Draw
                            odds['X'] = coefficient
                        elif bet_type == 3:  # Away win
                            odds['2'] = coefficient
        except Exception as e:
            self.logger.warning(f"Error extracting odds: {e}")
        
        return odds
    
    def filter_by_leagues(self, matches: List[Dict]) -> List[Dict]:
        """Filter matches by target leagues"""
        filtered_matches = []
        
        for match in matches:
            league = match.get('league', '').lower()
            
            # Special handling for MLS
            if 'usa. mls' in league:
                if league.strip() == 'usa. mls':
                    filtered_matches.append(match)
            else:
                # Check other target leagues
                for target in self.target_leagues:
                    if target.lower() in league:
                        filtered_matches.append(match)
                        break
        
        return filtered_matches
    
    def process_matches(self) -> List[Dict]:
        """Process all matches from API"""
        raw_data = self.fetch_api_data()
        
        if not raw_data or not isinstance(raw_data, list):
            self.logger.warning("No valid data received from API")
            return []
        
        processed_matches = []
        
        for match_data in raw_data:
            match_info = self.extract_match_info(match_data)
            if match_info:
                processed_matches.append(match_info)
        
        # Filter by target leagues
        filtered_matches = self.filter_by_leagues(processed_matches)
        
        self.logger.info(f"Processed {len(processed_matches)} total matches, {len(filtered_matches)} match target leagues")
        
        return filtered_matches
    
    def display_matches(self, matches: List[Dict]):
        """Display processed matches in a readable format"""
        if not matches:
            print("No matches found for target leagues")
            return
        
        print(f"\n{'='*80}")
        print(f"LIVE MATCHES - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        for match in matches:
            print(f"\nLeague: {match['league']}")
            print(f"Match: {match['team1']} vs {match['team2']}")
            
            if match['match_phase'] > 0:
                print(f"Score: {match['score1']} - {match['score2']}")
                print(f"Status: {match['match_status']}")
            else:
                print(f"Status: {match['match_status']}")
                if match['start_time']:
                    print(f"Start Time: {match['start_time'].strftime('%H:%M')}")
            
            if match['odds']:
                odds_str = " | ".join([f"{k}: {v}" for k, v in match['odds'].items()])
                print(f"Odds: {odds_str}")
            
            if match['location']:
                print(f"Location: {match['location']}")
            
            print(f"ID: {match['id']}")
            print("-" * 60)
    
    def save_to_json(self, matches: List[Dict], filename: str = None):
        """Save processed matches to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data/matches_{timestamp}.json"
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        try:
            # Convert datetime objects to strings for JSON serialization
            serializable_matches = []
            for match in matches:
                match_copy = match.copy()
                if match_copy.get('start_time'):
                    match_copy['start_time'] = match_copy['start_time'].isoformat()
                if match_copy.get('fetched_at'):
                    match_copy['fetched_at'] = match_copy['fetched_at'].isoformat()
                serializable_matches.append(match_copy)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(serializable_matches, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Saved {len(matches)} matches to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {e}")
    
    def monitor_continuously(self, interval: int = 30):
        """Continuously monitor matches"""
        self.logger.info(f"Starting continuous monitoring (interval: {interval}s)")
        
        try:
            while True:
                matches = self.process_matches()
                
                if matches:
                    self.display_matches(matches)
                    self.save_to_json(matches)
                else:
                    print(f"No matches found - {datetime.now().strftime('%H:%M:%S')}")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            self.logger.info("Monitoring stopped by user")
        except Exception as e:
            self.logger.error(f"Error in continuous monitoring: {e}")


def main():
    """Main function"""
    processor = BettingAPIProcessor()
    
    print("1xBetHind API Data Processor")
    print("=" * 40)
    print("1. Process matches once")
    print("2. Monitor continuously")
    print("3. Exit")
    
    choice = input("\nSelect option (1-3): ").strip()
    
    if choice == "1":
        matches = processor.process_matches()
        processor.display_matches(matches)
        processor.save_to_json(matches)
        
    elif choice == "2":
        interval = input("Enter monitoring interval in seconds (default 30): ").strip()
        try:
            interval = int(interval) if interval else 30
        except ValueError:
            interval = 30
        
        processor.monitor_continuously(interval)
        
    elif choice == "3":
        print("Exiting...")
        
    else:
        print("Invalid choice")


if __name__ == "__main__":
    main()
