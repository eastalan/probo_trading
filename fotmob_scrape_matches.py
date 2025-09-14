import os
import re
import time
import datetime
import uuid as uuid_lib
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from utils.db.config_loader import get_db_config, get_db_connection, get_fotmob_config, get_app_config

# Load configuration
fotmob_config = get_fotmob_config()
app_config = get_app_config()

# --- Configuration ---
FOTMOB_BASE_URL = fotmob_config['base_url']
TARGET_DATE_STR = datetime.date.today().strftime("%Y%m%d")
TARGET_LEAGUES = fotmob_config['target_leagues']
BRAVE_APP_PATH = app_config['browser_path']
HEADLESS_MODE = app_config['headless_mode']

def setup_driver():
    options = Options()
    if HEADLESS_MODE:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1366x768")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("useAutomationExtension", False)
    # macOS specific options
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--remote-debugging-port=0")
    
    # Try different approaches to find a compatible driver
    attempts = [
        # First try: Use system Chrome with manual driver path
        {"use_brave": False, "use_system_driver": True},
        # Second try: Use Brave browser if available
        {"use_brave": True, "use_system_driver": False},
        # Third try: Use webdriver-manager with older version
        {"use_brave": False, "use_system_driver": False, "force_version": "139.0.7258.154"},
        # Fourth try: Use system Chrome without webdriver-manager
        {"use_brave": False, "use_system_driver": False},
    ]
    
    for i, attempt in enumerate(attempts, 1):
        try:
            options_copy = Options()
            # Copy all arguments
            for arg in options.arguments:
                options_copy.add_argument(arg)
            for key, value in options.experimental_options.items():
                options_copy.add_experimental_option(key, value)
                
            if attempt.get("use_brave") and os.path.exists(BRAVE_APP_PATH):
                options_copy.binary_location = BRAVE_APP_PATH
                print(f"Attempt {i}: Trying with Brave browser...")
            else:
                print(f"Attempt {i}: Trying with system Chrome...")
            
            # Try different service approaches
            if attempt.get("use_system_driver"):
                # Try to use system chromedriver if available
                system_paths = [
                    "/usr/local/bin/chromedriver",
                    "/opt/homebrew/bin/chromedriver",
                    "/usr/bin/chromedriver"
                ]
                service = None
                for path in system_paths:
                    if os.path.exists(path) and os.access(path, os.X_OK):
                        print(f"Using system chromedriver at: {path}")
                        service = ChromeService(executable_path=path)
                        break
                if not service:
                    print("No system chromedriver found, falling back to webdriver-manager")
                    service = ChromeService(ChromeDriverManager().install())
            elif attempt.get("force_version"):
                # Try specific version
                version = attempt["force_version"]
                print(f"Forcing ChromeDriver version: {version}")
                service = ChromeService(ChromeDriverManager(version=version).install())
            else:
                # Use webdriver-manager default
                service = ChromeService(ChromeDriverManager().install())
            
            driver = webdriver.Chrome(service=service, options=options_copy)
            driver.set_page_load_timeout(45)
            print("WebDriver setup successful!")
            return driver
            
        except Exception as e:
            print(f"Attempt {i} failed: {e}")
            # Clean up any partial driver instances
            try:
                if 'driver' in locals():
                    driver.quit()
            except:
                pass
            continue
    
    print("All WebDriver setup attempts failed.")
    print("\nTroubleshooting suggestions:")
    print("1. Install ChromeDriver manually: brew install chromedriver")
    print("2. Allow ChromeDriver in macOS Security & Privacy settings")
    print("3. Try running: xattr -d com.apple.quarantine /path/to/chromedriver")
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

def get_existing_match_ids():
    """Read existing match IDs from the database to avoid duplicates"""
    existing_ids = set()
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT match_id FROM fotmob_events WHERE match_id IS NOT NULL")
            results = cursor.fetchall()
            for row in results:
                if row['match_id']:
                    existing_ids.add(row['match_id'])
        connection.close()
        print(f"📊 Found {len(existing_ids)} existing match IDs in database")
    except Exception as e:
        print(f"Error reading existing match IDs from database: {e}")
    return existing_ids

def insert_match_to_database(match_data):
    """Insert or update match data directly to MySQL database"""
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # Parse match data
            date_str, league_name, home_team, away_team, ko_time, match_link, match_id, download_flag, has_ended, uuid = match_data
            
            # Convert date format from YYYYMMDD to YYYY-MM-DD
            if len(date_str) == 8 and date_str.isdigit():
                match_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                print(f"⚠️  Invalid date format: {date_str}")
                return False
            
            # Clean data
            kickoff_time = ko_time if ko_time != 'N/A' else None
            match_link_clean = match_link if match_link != 'N/A' else None
            match_id_clean = match_id if match_id != 'N/A' else None
            uuid_clean = uuid if uuid != 'N/A' else None
            download_flag_int = int(download_flag) if download_flag.isdigit() else 0
            has_ended_int = int(has_ended) if has_ended.isdigit() else 0
            
            # Insert or update
            insert_sql = """
            INSERT INTO fotmob_events 
            (match_date, league_name, home_team, away_team, kickoff_time, 
             match_link, match_id, uuid, download_flag, has_ended)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            league_name = VALUES(league_name),
            home_team = VALUES(home_team),
            away_team = VALUES(away_team),
            kickoff_time = VALUES(kickoff_time),
            match_link = VALUES(match_link),
            uuid = VALUES(uuid),
            download_flag = VALUES(download_flag),
            has_ended = VALUES(has_ended),
            updated_at = CURRENT_TIMESTAMP
            """
            
            cursor.execute(insert_sql, (
                match_date, league_name, home_team, away_team, 
                kickoff_time, match_link_clean, match_id_clean, uuid_clean, 
                download_flag_int, has_ended_int
            ))
            
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Error inserting match to database: {e}")
        return False

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
                    # Create match data tuple for database insertion
                    match_data = (
                        date_str,
                        league_name,
                        home_team,
                        away_team,
                        ko_time,
                        match_link,
                        match_id,
                        "0",  # download_flag
                        has_ended,
                        None  # UUID - left blank for FotMob scraper
                    )
                    all_match_data_lines.append(match_data)
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
    
    print("🏗️  FotMob Scraper with Direct MySQL Integration")
    print("📊 Connecting to MySQL database...")

    # Get existing match IDs to avoid duplicates
    print("Reading existing match IDs from database to avoid duplicates...")
    existing_match_ids = get_existing_match_ids()

    driver = setup_driver()
    if driver:
        try:
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            
            # Process each date
            for date_str in dates_to_scrape:
                print(f"--- FotMob Scraper Processing Date: {date_str} ---")
                match_data_list = scrape_fotmob_matches(driver, date_str)
                
                print(f"📋 Found {len(match_data_list)} matches for {date_str}")
                
                # Insert each match directly to database
                for match_data in match_data_list:
                    match_id = match_data[6]  # match_id is at index 6
                    
                    if match_id in existing_match_ids:
                        print(f"  ⏭️  Skipping duplicate: {match_data[2]} vs {match_data[3]} (ID: {match_id})")
                        total_skipped += 1
                        continue
                    
                    # Insert to database
                    if insert_match_to_database(match_data):
                        print(f"  ✅ Inserted: {match_data[2]} vs {match_data[3]} ({match_data[1]})")
                        existing_match_ids.add(match_id)  # Add to set to avoid duplicates in this batch
                        total_inserted += 1
                    else:
                        print(f"  ❌ Failed to insert: {match_data[2]} vs {match_data[3]}")
                
            print(f"\n📊 Database Update Summary:")
            print(f"  ✅ Inserted: {total_inserted} matches")
            print(f"  ⏭️  Skipped (duplicates): {total_skipped} matches")
            print(f"  📋 Total processed: {total_inserted + total_skipped} matches")
                
        finally:
            driver.quit()
    else:
        print("Failed to initialize the web driver.")