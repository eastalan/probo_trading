import requests
import time
import datetime
import os
import re
import logging
import pdb  # For debugging purposes, remove in production
from log_utils import get_dated_log_path

def extract_team_names_from_response(data):
    """
    Extracts team names from API response.
    Returns: 'team1-vs-team2' format for log filename
    """
    try:
        value_data = data.get('Value', {})
        team1 = value_data.get('O1CT', '').replace(' ', '').lower()
        team2 = value_data.get('O2CT', '').replace(' ', '').lower()
        if team1 and team2:
            return f"{team1}-vs-{team2}"
    except (AttributeError, KeyError):
        pass
    return "melbet"  # fallback name

def parse_curl(curl_command):
    """
    Parses a cURL command string and returns (url, headers_dict, cookies_dict).
    """
    url = ""
    headers = {}
    cookies = {}
    # Extract URL
    url_match = re.search(r"curl ['\"]([^'\"]+)['\"]", curl_command)
    if url_match:
        url = url_match.group(1)
    # Extract headers
    header_matches = re.findall(r"-H ['\"]([^'\"]+)['\"]", curl_command)
    for h in header_matches:
        if ":" in h:
            k, v = h.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    # Extract cookies
    cookie_str = headers.get("cookie") or headers.get("cookies")
    if cookie_str:
        for pair in cookie_str.split(";"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                cookies[k.strip()] = v.strip()
        headers.pop("cookie", None)
        headers.pop("cookies", None)
    return url, headers, cookies

def poll_data(url, headers, cookies):
    try:
        resp = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        data = resp.json()
        # Use the correct path for Melbet API response
        e_list = data.get('Value', {}).get('GE', [{}])[0].get('E', [])
        c_values = []
        for entry in e_list:
            # Each entry is a list with a dict inside
            if entry and isinstance(entry, list) and "C" in entry[0]:
                c_values.append(entry[0]["C"])
                if len(c_values) == 3:
                    break
        return c_values, data  # Return both c_values and full data
    except Exception as e:
        logging.error(f"Error: {e}")
        return None, None

def print_if_changed(c_values, last_c_values, logger):
    if c_values is not None and c_values != last_c_values:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = f"[{now}] C values: {', '.join(str(c) for c in c_values)}"
        print(msg)
        logger.info(msg)
        return c_values
    return last_c_values

def main():
    # Use simple URL approach
    url = "https://melbet-india.net/service-api/LiveFeed/GetGameZip?id=647639490&lng=en&isSubGames=true&GroupEvents=true&countevents=250&grMode=4&partner=8&topGroups=&country=71&marketType=1"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    cookies = {}
    
    # Get initial data to extract team names for log filename
    print(f"Fetching initial data from: {url}")
    c_values, initial_data = poll_data(url, headers, cookies)
    
    if initial_data:
        match_name = extract_team_names_from_response(initial_data)
    else:
        match_name = "melbet"
    
    log_filename = f"{match_name}.log"
    
    # Setup logging with team-specific filename
    log_path = get_dated_log_path("melbet_data", log_filename)
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logger = logging.getLogger("melbet_logger")
    
    print(f"Logging to: {log_path}")
    
    if not url:
        print("Could not parse URL.")
        return

    print(f"Polling URL: {url}")
    last_c_values = None
    while True:
        c_values, _ = poll_data(url, headers, cookies)
        last_c_values = print_if_changed(c_values, last_c_values, logger)
        time.sleep(0.1)

if __name__ == "__main__":
    main()