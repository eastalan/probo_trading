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
    "France - Ligue 1"
]
OUTPUT_DIRECTORY = os.path.join("data", "event_data")
OUTPUT_FILE_NAME = "fotmob_matches.psv"
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIRECTORY, OUTPUT_FILE_NAME)
DELIMITER = "|"
BRAVE_APP_PATH = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"
HEADLESS_MODE = True

def setup_driver():
    options = Options()
    if os.path.exists(BRAVE_APP_PATH):
        options.binary_location = BRAVE_APP_PATH
    if HEADLESS_MODE:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1366x768")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(45)
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver: {e}")
        return None

def extract_match_id_from_url(url_str):
    match = re.search(r'#(\d+)$', url_str)
    if match:
        return match.group(1)
    match = re.search(r'/(\d+)(?:/[^/#]*)?(?:#\d*)?$', url_str)
    if match:
        return match.group(1)
    return "N/A"

def scrape_fotmob_matches(driver, date_str):
    url = f"{FOTMOB_BASE_URL}?date={date_str}"
    print(f"Navigating to FotMob for date: {date_str} - URL: {url}")
    driver.get(url)
    time.sleep(5)  # Wait for page to load

    all_match_data_lines = []
    # Find all league columns
    league_columns = driver.find_elements(By.CSS_SELECTOR, "div.css-121tc0y-Column-LeaguesColumnCSS")
    if not league_columns:
        print("No league columns found.")
        return []

    for league_column in league_columns:
        # For each league card in the column
        league_cards = league_column.find_elements(By.CSS_SELECTOR, "div.css-1lleae-CardCSS")
        for card in league_cards:
            # Get league name
            try:
                league_name_elem = card.find_element(By.CSS_SELECTOR, "div.css-170egrx-GroupTitle")
                league_name = league_name_elem.text.strip()
            except NoSuchElementException:
                continue
            # Only process target leagues (exact match)
            if league_name not in TARGET_LEAGUES:
                continue

            print(f"Processing league: {league_name}")
            # Find all matches in this league
            matches = card.find_elements(By.CSS_SELECTOR, "a.css-1ajdexg-MatchWrapper")
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
                    except NoSuchElementException:
                        ko_time = "N/A"
                    # Compose line with download_flag = 0
                    match_line_data = [
                        date_str,
                        league_name,
                        home_team,
                        away_team,
                        ko_time,
                        match_link,
                        match_id,
                        "0"  # download_flag
                    ]
                    match_line = DELIMITER.join(match_line_data)
                    all_match_data_lines.append(match_line + "\n")
                    print(f"  {home_team} vs {away_team} at {ko_time} ({match_id})")
                except Exception as e:
                    print(f"  Error extracting match: {e}")
                    continue
    return all_match_data_lines

if __name__ == "__main__":
    print(f"--- Fotmob Scraper Initializing for Date: {TARGET_DATE_STR} ---")
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)
    print(f"Output will be saved to: {OUTPUT_FILE_PATH}")

    driver = setup_driver()
    if driver:
        try:
            lines = scrape_fotmob_matches(driver, TARGET_DATE_STR)
            with open(OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
                header = DELIMITER.join([
                    "MatchDate", "LeagueName", "HomeTeam", "AwayTeam",
                    "KickOffTime", "MatchLink", "MatchID", "DownloadFlag"
                ]) + "\n"
                f.write(header)
                for line in lines:
                    f.write(line)
            print(f"\nSuccessfully wrote {len(lines)} match data lines to: {OUTPUT_FILE_PATH}")
        finally:
            driver.quit()
    else:
        print("Failed to initialize the web driver.")