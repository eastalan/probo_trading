#!/usr/bin/env python3
"""
FotMob UUID Extractor - Separate program to extract EVENT_UUID from FotMob match pages
"""
import os
import sys
import re
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def extract_event_uuid(match_url, max_retries=20, retry_delay=30):
    """
    Extract EVENT_UUID from FotMob playbyplay page with retry logic.
    """
    import time
    
    for attempt in range(max_retries):
        try:
            # Setup Chrome options for headless browsing
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Initialize WebDriver
            driver = webdriver.Chrome(service=webdriver.chrome.service.Service(ChromeDriverManager().install()), options=chrome_options)
            
            try:
                print(f"Attempt {attempt + 1}/{max_retries}: Loading match page: {match_url}")
                driver.get(match_url)
                
                # Wait for page to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Get page source and search for EVENT_UUID
                page_source = driver.page_source
                print(f"Page loaded, source length: {len(page_source)} characters")
                
                # Look for superliveurl iframe first (most reliable for live matches)
                iframe_pattern = r'<iframe[^>]*title="superLive"[^>]*src="[^"]*matchid=([a-z0-9]+)[^"]*"[^>]*>'
                iframe_matches = re.findall(iframe_pattern, page_source, re.IGNORECASE)
                if iframe_matches:
                    event_uuid = iframe_matches[0]
                    print(f"Found EVENT_UUID from superliveurl iframe: {event_uuid}")
                    return event_uuid
                
                # Alternative iframe pattern
                iframe_pattern2 = r'matchid=([a-z0-9]+)&'
                iframe_matches2 = re.findall(iframe_pattern2, page_source, re.IGNORECASE)
                if iframe_matches2:
                    event_uuid = iframe_matches2[0]
                    print(f"Found EVENT_UUID from iframe matchid parameter: {event_uuid}")
                    return event_uuid
                
                # Fallback patterns for other cases
                patterns = [
                    r'"eventuuid"\s*:\s*"([a-z0-9]+)"',
                    r'"eventUuid"\s*:\s*"([a-z0-9]+)"',
                    r'"event_uuid"\s*:\s*"([a-z0-9]+)"',
                    r'eventuuid["\']?\s*[:=]\s*["\']([a-z0-9]+)["\']',
                    r'eventUuid["\']?\s*[:=]\s*["\']([a-z0-9]+)["\']'
                ]
                
                for pattern in patterns:
                    matches = re.findall(pattern, page_source, re.IGNORECASE)
                    if matches:
                        event_uuid = matches[0]
                        print(f"Found EVENT_UUID from page pattern: {event_uuid}")
                        return event_uuid
                
                # If not found in page source, try to find in script tags
                script_elements = driver.find_elements(By.TAG_NAME, "script")
                for script in script_elements:
                    script_content = script.get_attribute("innerHTML")
                    if script_content:
                        for pattern in patterns:
                            matches = re.findall(pattern, script_content, re.IGNORECASE)
                            if matches:
                                event_uuid = matches[0]
                                print(f"Found EVENT_UUID in script: {event_uuid}")
                                return event_uuid
                
                # If iframe patterns not found and this isn't the last attempt, retry
                if attempt < max_retries - 1:
                    print(f"EVENT_UUID iframe not found, retrying in {retry_delay} seconds... ({attempt + 1}/{max_retries})")
                else:
                    print("EVENT_UUID not found in page after all attempts")
                    return None
                
            finally:
                driver.quit()
                
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return None
        
        # Sleep before retry (except on last attempt)
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
    
    return None

def main():
    parser = argparse.ArgumentParser(description='Extract EVENT_UUID from FotMob match page')
    parser.add_argument('match_url', help='FotMob match URL')
    parser.add_argument('--match-id', help='Match ID for reference')
    
    args = parser.parse_args()
    
    print(f"=== FotMob UUID Extractor ===")
    if args.match_id:
        print(f"Match ID: {args.match_id}")
    
    event_uuid = extract_event_uuid(args.match_url)
    
    if event_uuid:
        print(f"SUCCESS: EVENT_UUID = {event_uuid}")
        return 0
    else:
        print("FAILED: Could not extract EVENT_UUID")
        return 1

if __name__ == "__main__":
    exit(main())
