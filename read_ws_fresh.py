import time
import os
import re # For slugify and class regex
import datetime # For timestamps and file headers
import platform # To help determine OS for profile paths
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import pdb
import tempfile

# --- User Configuration for this Specific Script ---

# **IMPORTANT**: This is pre-filled for macOS.
# If you are on WINDOWS or LINUX, you MUST change this path to your Brave browser executable.
BRAVE_BINARY_PATH = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser" # macOS
# Example for Windows:
# BRAVE_BINARY_PATH = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"
# Example for Linux (find with 'which brave-browser'):
# BRAVE_BINARY_PATH = "/usr/bin/brave-browser"

FOTMOB_MATCH_URL = "https://www.fotmob.com/en-GB/match/4694438/playbyplay" # Example, change if needed
POLL_INTERVAL = 0.2 # As per your last script
INITIAL_PAGE_LOAD_WAIT = 3 # As per your last script
HEADLESS_BROWSER = False # Set to True if you want to run in headless mode
LOG_OUTPUT_DIR = "fotmob_opta_event_logs"

# --- End of User Configuration ---

def slugify_filename(text):
    if not text: return "unknown_match_events"
    text = str(text).lower()
    text = re.sub(r'\s+', '_', text)
    text = re.sub(r'[^\w_.-]', '', text)
    return text[:100]

def setup_brave_driver(brave_exe_path):
    options = ChromeOptions()
    if not brave_exe_path:
        print("Error: BRAVE_BINARY_PATH is empty. Please set it.")
        return None, None
    if not os.path.exists(brave_exe_path):
        print(f"Error: Brave binary path specified but not found: {brave_exe_path}")
        return None, None
    options.binary_location = brave_exe_path
    print(f"Using Brave browser binary at: {brave_exe_path}")

    # Use a temporary user data directory for each driver instance
    temp_user_data_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={temp_user_data_dir}")
    print(f"Using temporary Brave profile at: {temp_user_data_dir}")

    if HEADLESS_BROWSER:
        options.add_argument("--headless=new")
        print("Running Brave in headless mode.")
    else:
        options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-dev-shm-usage")
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(INITIAL_PAGE_LOAD_WAIT + 10)
        return driver, temp_user_data_dir
    except WebDriverException as e:
        print(f"WebDriverException setting up Brave WebDriver: {e}")
        return None, None
    except Exception as e:
        print(f"Error setting up Brave WebDriver: {e}")
        return None, None

def extract_event_details_from_page(driver_instance):
    """
    Extracts details from the first complete Opta-EventBannerInner in the superLive iframe.
    Returns a formatted string and a boolean indicating success.
    """
    formatted_event_string = None
    event_data_found = False
    status_message = "No relevant event banners found."

    try:
        driver_instance.switch_to.default_content()
        iframe_element = driver_instance.find_element(By.CSS_SELECTOR, "iframe[title='superLive']")
        driver_instance.switch_to.frame(iframe_element)
        iframe_html_content = driver_instance.page_source
        soup = BeautifulSoup(iframe_html_content, 'html.parser')

        # Find all potential event banners
        all_banner_inners = soup.find_all('div', class_='Opta-EventBannerInner')

        if not all_banner_inners:
            status_message = "No Opta-EventBannerInner elements found in iframe."
        else:
            for inner_banner in all_banner_inners:
                team_name = "Unknown Team"
                event_type = "" # Must have an event type
                player_name = ""

                # Extract Team Name from img alt/title
                # Look for an image holder that contains a team badge image
                team_icon_holder = inner_banner.find('div', class_='Opta-EventIcon')
                if team_icon_holder:
                    img_tag = team_icon_holder.find('img', class_=re.compile(r'Opta-Image-Team-'))
                    if img_tag:
                        team_name_alt = img_tag.get('alt', '').strip()
                        team_name_title = img_tag.get('title', '').strip()
                        if team_name_alt:
                            team_name = team_name_alt
                        elif team_name_title:
                            team_name = team_name_title

                # Extract Event Type
                event_text_div = inner_banner.find('div', class_='Opta-EventText')
                if event_text_div:
                    event_header_div = event_text_div.find('div', class_='Opta-EventHeader')
                    if event_header_div:
                        span_tag_header = event_header_div.find('span')
                        if span_tag_header and span_tag_header.string:
                            event_type = span_tag_header.string.strip()

                    # Extract Player Name
                    event_details_div = event_text_div.find('div', class_='Opta-EventDetails')
                    if event_details_div:
                        span_tag_details = event_details_div.find('span')
                        if span_tag_details and span_tag_details.string:
                            player_name = span_tag_details.string.strip()

                if event_type: # A valid event must have an event_type
                    event_data_found = True
                    formatted_event_string = f"{team_name} - Event: {event_type}"
                    if player_name:
                        formatted_event_string += f" ; {player_name}"
                    status_message = "Event data extracted."
                    break # Process the first valid banner found

            if not event_data_found: # If loop finished without finding valid event_type
                status_message = "Found banners, but no valid event type extracted."


        driver_instance.switch_to.default_content()
        return formatted_event_string, event_data_found, status_message

    except NoSuchElementException:
        try: driver_instance.switch_to.default_content();
        except: pass
        return None, False, "Error: superLive iframe not found."
    except Exception as e:
        try: driver_instance.switch_to.default_content();
        except: pass
        return None, False, f"Error extracting details: {str(e)}"


def get_teams_and_date_from_header(driver_instance):
    """
    Extracts team names and match date from the TeamsHeader section.
    Returns (team1, team2, match_date_str, live_time_str)
    """
    try:
        driver_instance.switch_to.default_content()
        soup = BeautifulSoup(driver_instance.page_source, 'html.parser')
        header = soup.find('section', class_=re.compile(r"TeamsHeader"))
        if not header:
            return None, None, None, None

        # Team names
        team_spans = header.find_all('span', class_=re.compile(r'TeamNameItself-TeamNameOnTabletUp'))
        if len(team_spans) >= 2:
            team1 = team_spans[0].get_text(strip=True)
            team2 = team_spans[1].get_text(strip=True)
        else:
            # Fallback: try mobile class
            team_spans = header.find_all('span', class_=re.compile(r'TeamNameItself-TeamNameOnMobile'))
            team1 = team_spans[0].get_text(strip=True) if len(team_spans) > 0 else "Team1"
            team2 = team_spans[1].get_text(strip=True) if len(team_spans) > 1 else "Team2"

        # Match date (fallback to today if not found)
        match_date = datetime.datetime.now().strftime("%Y%m%d")

        # Live time
        live_time_span = header.find('span', class_=re.compile(r'MFStatusLiveTimeText'))
        live_time_str = live_time_span.get_text(strip=True) if live_time_span else ""

        return team1, team2, match_date, live_time_str
    except Exception as e:
        return "Team1", "Team2", datetime.datetime.now().strftime("%Y%m%d"), ""

def monitor_fotmob_events():
    driver, temp_profile_dir = setup_brave_driver(BRAVE_BINARY_PATH)
    if not driver:
        print("Failed to initialize Brave WebDriver. Exiting.")
        return

    if not os.path.exists(LOG_OUTPUT_DIR):
        try:
            os.makedirs(LOG_OUTPUT_DIR); print(f"Created log directory: {LOG_OUTPUT_DIR}")
        except OSError as e:
            print(f"Error creating log directory {LOG_OUTPUT_DIR}: {e}. Exiting.")
            if driver: driver.quit(); return

    # --- Extract team names and date for log filename ---
    print("Loading page to extract team names and date for log filename...")
    try:
        driver.get(FOTMOB_MATCH_URL)
        time.sleep(INITIAL_PAGE_LOAD_WAIT)
    except Exception as e:
        print(f"Error loading page for team extraction: {e}")
        if driver: driver.quit(); return

    team1, team2, match_date, _ = get_teams_and_date_from_header(driver)
    team1_slug = slugify_filename(team1)
    team2_slug = slugify_filename(team2)
    log_filename = f"{team1_slug}_{team2_slug}_{match_date}.log"
    log_filepath = os.path.join(LOG_OUTPUT_DIR, log_filename)
    print(f"Logging Opta events to: {log_filepath}")

    if not os.path.exists(log_filepath) or os.path.getsize(log_filepath) == 0:
        try:
            with open(log_filepath, 'w', encoding='utf-8') as f:
                f.write(f"# Opta events for match: {FOTMOB_MATCH_URL}\n")
                f.write(f"# Teams: {team1} vs {team2}\n")
                f.write(f"# Log session started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("#----------------------------------------------------------\n")
            print(f"Created new log file with header: {log_filepath}")
        except Exception as e:
            print(f"Error creating log file header: {e}")

    print(f"Opening FotMob page: {FOTMOB_MATCH_URL}")
    try:
        driver.get(FOTMOB_MATCH_URL)
        print(f"Waiting for initial page load ({INITIAL_PAGE_LOAD_WAIT} seconds)...")
        time.sleep(INITIAL_PAGE_LOAD_WAIT)
        print("Initial load potentially complete. Starting polling.")
    except TimeoutException:
        print(f"Timeout loading page {FOTMOB_MATCH_URL}.")
        if driver: driver.quit(); return
    except Exception as e:
        print(f"Error loading page {FOTMOB_MATCH_URL}: {e}")
        if driver: driver.quit(); return

    last_event_string_for_console = None
    last_event_string_for_file = None
    consecutive_not_found_count = 0
    MAX_CONSECUTIVE_NOT_FOUND_BEFORE_CONSOLE_WARNING = 10

    try:
        while True:
            # Get live time from header for each event
            _, _, _, live_time_str = get_teams_and_date_from_header(driver)
            current_formatted_event, event_found, status_msg = extract_event_details_from_page(driver)
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

            # --- Live clock checks ---
            if not live_time_str:
                print(f"[{timestamp}] Live clock not active. Pausing event polling. Will retry every 10 seconds...")
                while True:
                    time.sleep(10)
                    _, _, _, live_time_str = get_teams_and_date_from_header(driver)
                    if live_time_str:
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Live clock resumed: {live_time_str}. Resuming event polling.")
                        break
                    if live_time_str and live_time_str.strip().lower() in ["full time", "ft"]:
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Match ended ({live_time_str}). Terminating program.")
                        return

            if live_time_str.strip().lower() in ["full time", "ft"]:
                print(f"[{timestamp}] Match ended ({live_time_str}). Terminating program.")
                break

            # Append live time to event output
            live_time_display = f" [Live: {live_time_str}]" if live_time_str else ""

            if event_found and current_formatted_event:
                consecutive_not_found_count = 0
                if current_formatted_event != last_event_string_for_console:
                    print(f"[{timestamp}]{live_time_display} {current_formatted_event}")
                    last_event_string_for_console = current_formatted_event

                if current_formatted_event != last_event_string_for_file:
                    try:
                        with open(log_filepath, 'a', encoding='utf-8') as f:
                            f.write(f"[{timestamp}]{live_time_display} {current_formatted_event}\n")
                        last_event_string_for_file = current_formatted_event
                    except Exception as e:
                        print(f"Error writing to log file {log_filepath}: {e}")
            else:
                consecutive_not_found_count += 1
                if consecutive_not_found_count == 1 or \
                   consecutive_not_found_count % MAX_CONSECUTIVE_NOT_FOUND_BEFORE_CONSOLE_WARNING == 0:
                    print(f"[{timestamp}] Update: {status_msg} (Attempt {consecutive_not_found_count})")

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"Critical error in monitoring loop: {e}")
    finally:
        print("Closing Brave browser.")
        if driver:
            driver.quit()
        if temp_profile_dir:
            try:
                import shutil
                shutil.rmtree(temp_profile_dir)
                print(f"Removed temporary profile directory: {temp_profile_dir}")
            except OSError as e:
                print(f"Error removing temporary profile directory {temp_profile_dir}: {e}")
        print(f"Event logging to {log_filepath} complete.")

if __name__ == "__main__":
    if not BRAVE_BINARY_PATH:
        print("="*70+"\n!!! ACTION REQUIRED: BRAVE_BINARY_PATH IS EMPTY !!!\n"+"="*70)
    elif not os.path.exists(BRAVE_BINARY_PATH):
        print(f"\n{'='*70}\n!!! ACTION REQUIRED: BRAVE_BINARY_PATH NOT FOUND at '{BRAVE_BINARY_PATH}' !!!\n{'='*70}")
    else:
        monitor_fotmob_events()