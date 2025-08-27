import os
import re
import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# --- Configuration ---
FOTMOB_BASE_URL = "https://www.fotmob.com/"
TARGET_DATE_STR = datetime.date.today().strftime("%Y%m%d")
TARGET_LEAGUES = [
    "England - Premier League",
    "Spain - LaLiga",
    "Germany - Bundesliga",
    "Italy - Serie A",
    "France - Ligue 1",
    "Japan - J. League",
    "United States - Major League Soccer"
]
OUTPUT_DIRECTORY = os.path.join("data", "event_data")
OUTPUT_FILE_NAME = "fotmob_matches.psv"
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIRECTORY, OUTPUT_FILE_NAME)
DELIMITER = "|"
BRAVE_APP_PATH = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"
HEADLESS_MODE = True

def setup_driver():
    options = Options()
    if HEADLESS_MODE:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1366x768")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("useAutomationExtension", False)
    
    # Try different approaches to find a compatible driver
    attempts = [
        # First try: Use system Chrome without specifying binary
        {"use_brave": False, "driver_version": None},
        # Second try: Use Brave browser if available
        {"use_brave": True, "driver_version": None},
        # Third try: Force latest driver with system Chrome
        {"use_brave": False, "driver_version": "latest"},
    ]
    
    for attempt in attempts:
        try:
            options_copy = Options()
            # Copy all arguments
            for arg in options.arguments:
                options_copy.add_argument(arg)
            for key, value in options.experimental_options.items():
                options_copy.add_experimental_option(key, value)
                
            if attempt["use_brave"] and os.path.exists(BRAVE_APP_PATH):
                options_copy.binary_location = BRAVE_APP_PATH
                print("Trying with Brave browser...")
            else:
                print("Trying with system Chrome...")
            
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options_copy)
            driver.set_page_load_timeout(45)
            print("WebDriver setup successful!")
            return driver
            
        except Exception as e:
            print(f"Attempt failed: {e}")
            continue
    
    print("All WebDriver setup attempts failed.")
    return None

def extract_match_id_from_url(url_str):
    match = re.search(r'#(\d+)$', url_str)
    if match:
        return match.group(1)
    match = re.search(r'/(\d+)(?:/[^/#]*)?(?:#\d*)?$', url_str)
    if match:
        return match.group(1)
    return "N/A"

def convert_to_24hr_format(time_str):
    """Convert time string to 24-hour format"""
    if not time_str or time_str.upper() == "N/A":
        return "N/A"
    
    try:
        # Handle various time formats
        time_str = time_str.strip()
        
        # If already in 24-hour format (contains :)
        if ':' in time_str and ('AM' not in time_str.upper() and 'PM' not in time_str.upper()):
            return time_str
            
        # Handle AM/PM format
        if 'AM' in time_str.upper() or 'PM' in time_str.upper():
            # Extract time and AM/PM
            time_part = re.search(r'(\d{1,2}):(\d{2})', time_str)
            am_pm = 'PM' if 'PM' in time_str.upper() else 'AM'
            
            if time_part:
                hour = int(time_part.group(1))
                minute = int(time_part.group(2))
                
                if am_pm == 'PM' and hour != 12:
                    hour += 12
                elif am_pm == 'AM' and hour == 12:
                    hour = 0
                    
                return f"{hour:02d}:{minute:02d}"
        
        # Handle time without AM/PM (assume it's already 24hr or needs context)
        time_match = re.search(r'(\d{1,2}):(\d{2})', time_str)
        if time_match:
            return f"{int(time_match.group(1)):02d}:{time_match.group(2)}"
            
        return time_str  # Return original if can't parse
        
    except Exception as e:
        print(f"Error converting time '{time_str}': {e}")
        return time_str

def get_has_ended_status(status_text):
    # You may want to improve this logic based on actual status text on the page
    if status_text.strip().lower() in ["ft", "full time", "finished", "ended"]:
        return "1"
    return "0"

def get_existing_match_ids(file_path):
    """Read existing match IDs from the PSV file to avoid duplicates"""
    existing_ids = set()
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith("MatchDate"):  # Skip header
                        parts = line.split(DELIMITER)
                        if len(parts) >= 7:  # Ensure we have enough columns including MatchID
                            match_id = parts[6].strip()  # MatchID is at index 6
                            if match_id and match_id != "N/A":
                                existing_ids.add(match_id)
        except Exception as e:
            print(f"Error reading existing match IDs: {e}")
    return existing_ids

def filter_duplicate_matches(match_lines, existing_match_ids):
    """Filter out matches that already exist based on MatchID"""
    unique_matches = []
    duplicate_count = 0
    
    for line in match_lines:
        parts = line.strip().split(DELIMITER)
        if len(parts) >= 7:
            match_id = parts[6].strip()  # MatchID is at index 6
            if match_id not in existing_match_ids:
                unique_matches.append(line)
                existing_match_ids.add(match_id)  # Add to set to avoid duplicates within this batch
            else:
                duplicate_count += 1
                print(f"  Skipping duplicate match ID: {match_id}")
    
    if duplicate_count > 0:
        print(f"  Filtered out {duplicate_count} duplicate matches")
    
    return unique_matches

def scrape_fotmob_matches(driver, date_str):
    url = f"{FOTMOB_BASE_URL}?date={date_str}"
    print(f"Navigating to FotMob for date: {date_str} - URL: {url}")
    driver.get(url)
    time.sleep(5)  # Wait for page to load

    all_match_data_lines = []
    # Find all league columns
    league_columns = driver.find_elements(By.CSS_SELECTOR, "div.css-121tc0y-Column-LeaguesColumnCSS")
    print(f"Found {len(league_columns)} league columns")
    
    if not league_columns:
        print("No league columns found. Trying alternative selectors...")
        # Try alternative selectors
        league_columns = driver.find_elements(By.CSS_SELECTOR, "[data-testid='league-column']")
        if not league_columns:
            league_columns = driver.find_elements(By.CSS_SELECTOR, "div[class*='Column']")
        print(f"Found {len(league_columns)} league columns with alternative selectors")
        
    if not league_columns:
        print("Still no league columns found. Page might have different structure.")
        return []

    for i, league_column in enumerate(league_columns):
        print(f"Processing league column {i+1}/{len(league_columns)}")
        # For each league card in the column
        league_cards = league_column.find_elements(By.CSS_SELECTOR, "div.css-1lleae-CardCSS")
        if not league_cards:
            league_cards = league_column.find_elements(By.CSS_SELECTOR, "div[class*='Card']")
        print(f"  Found {len(league_cards)} league cards in column {i+1}")
        
        for j, card in enumerate(league_cards):
            # Get league name
            try:
                league_name_elem = card.find_element(By.CSS_SELECTOR, "div.css-170egrx-GroupTitle")
                league_name = league_name_elem.text.strip()
            except NoSuchElementException:
                try:
                    league_name_elem = card.find_element(By.CSS_SELECTOR, "[class*='GroupTitle']")
                    league_name = league_name_elem.text.strip()
                except NoSuchElementException:
                    print(f"    Card {j+1}: Could not find league name")
                    continue
            
            print(f"    Card {j+1}: Found league '{league_name}'")
            
            # Only process target leagues (exact match)
            if league_name not in TARGET_LEAGUES:
                # Don't print skip message for every league to reduce noise
                continue

            print(f"Processing league: {league_name}")
            # Find all matches in this league
            matches = card.find_elements(By.CSS_SELECTOR, "a.css-1ajdexg-MatchWrapper")
            if not matches:
                matches = card.find_elements(By.CSS_SELECTOR, "a[class*='MatchWrapper']")
            print(f"  Found {len(matches)} matches in {league_name}")
            
            for match in matches:
                try:
                    # Match link
                    match_link = match.get_attribute("href")
                    if not match_link.startswith("http"):
                        match_link = FOTMOB_BASE_URL.rstrip("/") + match_link
                    match_id = extract_match_id_from_url(match_link)
                    # Home team
                    home_team_elem = match.find_element(By.CSS_SELECTOR, ".css-9871a0-StatusAndHomeTeamWrapper .css-1o142s8-TeamName")
                    home_team = home_team_elem.text.strip()
                    # Away team
                    away_team_elem = match.find_element(By.CSS_SELECTOR, ".css-gn249o-AwayTeamAndFollowWrapper .css-1o142s8-TeamName")
                    away_team = away_team_elem.text.strip()
                    # KO time
                    try:
                        ko_time_elem = match.find_element(By.CSS_SELECTOR, ".css-hytar5-TimeCSS")
                        ko_time = ko_time_elem.text.strip().replace("\n", " ")
                        # Add PM/AM if present
                        try:
                            ampm = ko_time_elem.find_element(By.CSS_SELECTOR, ".css-xhwcu3-AdjustedFontSize").text.strip()
                            ko_time = f"{ko_time} {ampm}"
                        except NoSuchElementException:
                            pass
                        # Convert to 24-hour format
                        ko_time = convert_to_24hr_format(ko_time)
                    except NoSuchElementException:
                        ko_time = "N/A"
                    # Status (for HasEnded)
                    try:
                        status_elem = match.find_element(By.CSS_SELECTOR, ".css-1w6y4ye-Status")
                        status_text = status_elem.text.strip()
                    except NoSuchElementException:
                        status_text = ""
                    has_ended = get_has_ended_status(status_text)
                    # Compose line with download_flag = 0 and has_ended
                    match_line_data = [
                        date_str,
                        league_name,
                        home_team,
                        away_team,
                        ko_time,
                        match_link,
                        match_id,
                        "0",         # download_flag
                        has_ended    # has_ended
                    ]
                    match_line = DELIMITER.join(match_line_data)
                    all_match_data_lines.append(match_line + "\n")
                    print(f"  {home_team} vs {away_team} at {ko_time} ({match_id}) | HasEnded: {has_ended}")
                except Exception as e:
                    print(f"  Error extracting match: {e}")
                    continue
    return all_match_data_lines

if __name__ == "__main__":
    today = datetime.date.today()
    dates_to_scrape = [
        today.strftime("%Y%m%d"),
        (today + datetime.timedelta(days=1)).strftime("%Y%m%d")
    ]
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)
    print(f"Output will be appended to: {OUTPUT_FILE_PATH}")

    # Get existing match IDs to avoid duplicates
    print("Reading existing match IDs to avoid duplicates...")
    existing_match_ids = get_existing_match_ids(OUTPUT_FILE_PATH)
    print(f"Found {len(existing_match_ids)} existing match IDs")

    driver = setup_driver()
    if driver:
        try:
            file_exists = os.path.exists(OUTPUT_FILE_PATH)
            all_new_matches = []
            
            # Collect all matches first
            for date_str in dates_to_scrape:
                print(f"--- Fotmob Scraper Initializing for Date: {date_str} ---")
                lines = scrape_fotmob_matches(driver, date_str)
                
                # Process each line (fix N/A times)
                processed_lines = []
                for line in lines:
                    parts = line.strip().split(DELIMITER)
                    if len(parts) == 9 and parts[4].strip().upper() == "N/A":
                        parts[8] = "1"
                        line = DELIMITER.join(parts) + "\n"
                    processed_lines.append(line)
                
                all_new_matches.extend(processed_lines)
            
            # Filter out duplicates
            print(f"\nFiltering duplicates from {len(all_new_matches)} scraped matches...")
            unique_matches = filter_duplicate_matches(all_new_matches, existing_match_ids)
            print(f"Found {len(unique_matches)} new unique matches to add")
            
            # Write to file
            if unique_matches:
                with open(OUTPUT_FILE_PATH, 'a', encoding='utf-8') as f:
                    if not file_exists:
                        header = DELIMITER.join([
                            "MatchDate", "LeagueName", "HomeTeam", "AwayTeam",
                            "KickOffTime", "MatchLink", "MatchID", "DownloadFlag", "HasEnded"
                        ]) + "\n"
                        f.write(header)
                    # Add a single new line before the first append if file already exists and is not empty
                    elif file_exists and os.path.getsize(OUTPUT_FILE_PATH) > 0:
                        f.write("\n")
                    
                    for line in unique_matches:
                        f.write(line)
                
                print(f"\nAppended {len(unique_matches)} new match data lines for {', '.join(dates_to_scrape)} to: {OUTPUT_FILE_PATH}")
            else:
                print(f"\nNo new matches to add - all scraped matches already exist in: {OUTPUT_FILE_PATH}")
                
        finally:
            driver.quit()
    else:
        print("Failed to initialize the web driver.")