#!/usr/bin/env python3
"""
Sportradar URL Ping Monitor
Pings the specified Sportradar URL and stores the response
"""

import requests
import json
import os
import time
from datetime import datetime
from log_utils import get_dated_log_path

# Target URL
SPORTRADAR_URL = "https://lmt.fn.sportradar.com/fanduel/en/Etc:UTC/gismo/match_timelinedelta/63293745?T=exp=1757273759~acl=/*~data=eyJvIjoiaHR0cHM6Ly92aWRlb3BsYXllci5iZXRmYWlyLmNvbSIsImEiOiJhZTdjM2E5ZGQ1OTkyZWRlN2FlOWQ2Njg0YzIxNTEyZiIsImFjdCI6Im9yaWdpbmNoZWNrIiwib3NyYyI6Im9yaWdpbiJ9~hmac=e654cf9fed5a5d1f068cd8c9260fd46cc8db0f82f7fab3000fb39ac04bdc2b47"

# Request headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://videoplayer.betfair.com/',
    'Origin': 'https://videoplayer.betfair.com'
}

def ping_sportradar_url():
    """Ping the Sportradar URL and extract events data"""
    try:
        print(f"Pinging Sportradar URL...")
        
        response = requests.get(SPORTRADAR_URL, headers=HEADERS, timeout=30)
        
        # Get response details
        status_code = response.status_code
        response_text = response.text
        
        print(f"Status Code: {status_code}")
        
        # Parse JSON and extract events
        events_data = None
        if status_code == 200:
            try:
                json_data = json.loads(response_text)
                
                # Extract events from the response structure
                if isinstance(json_data, dict) and 'doc' in json_data:
                    doc_list = json_data.get('doc', [])
                    if isinstance(doc_list, list) and len(doc_list) > 0:
                        match_data = doc_list[0].get('data', {})
                        events_data = match_data.get('events', [])
                        print(f"Extracted {len(events_data)} events")
                    else:
                        print("No doc list found or empty")
                        events_data = []
                elif isinstance(json_data, list) and len(json_data) > 0:
                    # Fallback for direct list format
                    match_data = json_data[0].get('data', {})
                    events_data = match_data.get('events', [])
                    print(f"Extracted {len(events_data)} events (direct list format)")
                else:
                    print("Response format not recognized")
                    events_data = []
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                print(f"Response text (first 200 chars): {response_text[:200]}")
                events_data = None
        
        return {
            'status_code': status_code,
            'events': events_data,
            'events_count': len(events_data) if events_data else 0,
            'timestamp': datetime.now().isoformat(),
            'url': SPORTRADAR_URL
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error pinging Sportradar URL: {e}"
        print(error_msg)
        return {
            'error': error_msg,
            'timestamp': datetime.now().isoformat(),
            'url': SPORTRADAR_URL
        }

def save_response_to_log(response_data):
    """Save the events data to a log file"""
    try:
        # Create log file path
        log_filename = "sportradar_events.log"
        log_path = get_dated_log_path('sportradar_data', log_filename)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        # Write events data to log file
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(f"[{response_data['timestamp']}] Events Data:\n")
            
            if 'error' in response_data:
                f.write(f"ERROR: {response_data['error']}\n")
            else:
                f.write(f"Status Code: {response_data['status_code']}\n")
                f.write(f"Events Count: {response_data['events_count']}\n")
                if response_data['events']:
                    f.write(f"Events:\n{json.dumps(response_data['events'], indent=2)}\n")
                else:
                    f.write("No events data\n")
            
            f.write("-" * 80 + "\n\n")
        
        print(f"Events saved to: {log_path}")
        
    except Exception as e:
        print(f"Error saving events to log: {e}")

def save_events_to_json(response_data):
    """Save only the events data to a JSON file for easy parsing"""
    try:
        # Create JSON file path
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"sportradar_events_{timestamp_str}.json"
        json_dir = os.path.join("data", "sportradar_events")
        os.makedirs(json_dir, exist_ok=True)
        json_path = os.path.join(json_dir, json_filename)
        
        # Write only events data to JSON file
        events_only = {
            'timestamp': response_data['timestamp'],
            'events_count': response_data['events_count'],
            'events': response_data['events'] if response_data['events'] else []
        }
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(events_only, f, indent=2, ensure_ascii=False)
        
        print(f"Events saved to JSON: {json_path}")
        
    except Exception as e:
        print(f"Error saving events to JSON: {e}")

def monitor_continuously(interval_seconds=0.5):
    """Continuously monitor the URL at specified intervals"""
    print(f"Starting continuous monitoring (interval: {interval_seconds} seconds)")
    print("Press Ctrl+C to stop")
    
    previous_response_hash = None
    
    try:
        while True:
            print(f"\n{'-'*60}")
            print(f"Monitoring at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Ping the URL
            response_data = ping_sportradar_url()
            
            # Create hash based only on event _id values to detect new events
            current_events = response_data.get('events')
            if current_events:
                # Extract only the _id values from events for comparison
                import hashlib
                event_ids = [event.get('_id') for event in current_events if event.get('_id')]
                event_ids.sort()  # Sort for consistent comparison
                ids_str = json.dumps(event_ids)
                current_response_hash = hashlib.md5(ids_str.encode()).hexdigest()
            else:
                current_response_hash = None
            
            # Only save if response has changed or it's the first run
            if current_response_hash != previous_response_hash:
                print("Response changed - saving data...")
                save_response_to_log(response_data)
                save_events_to_json(response_data)
                previous_response_hash = current_response_hash
            else:
                print("No changes in response data")
            
            # Wait for next ping
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
    except Exception as e:
        print(f"Error in continuous monitoring: {e}")

def main():
    """Main function - single ping or continuous monitoring"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        # Continuous monitoring mode
        interval = 0.5  # Default 0.5 seconds
        if len(sys.argv) > 2:
            try:
                interval = float(sys.argv[2])
            except ValueError:
                print("Invalid interval, using default 0.5 seconds")
        
        monitor_continuously(interval)
    else:
        # Single ping mode
        print("Single ping mode (use --continuous for monitoring)")
        response_data = ping_sportradar_url()
        save_response_to_log(response_data)
        save_events_to_json(response_data)

if __name__ == "__main__":
    main()
