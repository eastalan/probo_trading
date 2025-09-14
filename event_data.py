# event_data.py
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
import time
import os
import re 

from utils.core import config # Import global configurations

def setup_driver_for_event_lister():
    options = Options()
    try:
        # Only use a profile if specified in config
        if getattr(config, "BRAVE_PROFILE_TO_USE_EVENT_LISTER", None):
            home_directory = os.path.expanduser("~")
            brave_user_data_dir_base = os.path.join(home_directory, "Library/Application Support/BraveSoftware/")
            brave_user_data_dir = os.path.join(brave_user_data_dir_base, "Brave-Browser/")
            if not os.path.isdir(brave_user_data_dir):
                print(f"Error: Brave user data directory not found at: {brave_user_data_dir}")
                return None
            profile_path_to_check = os.path.join(brave_user_data_dir, config.BRAVE_PROFILE_TO_USE_EVENT_LISTER)
            if not os.path.isdir(profile_path_to_check):
                print(f"Error: Brave profile '{config.BRAVE_PROFILE_TO_USE_EVENT_LISTER}' not found within {brave_user_data_dir}")
                return None
            options.add_argument(f"user-data-dir={brave_user_data_dir}")
            options.add_argument(f"profile-directory={config.BRAVE_PROFILE_TO_USE_EVENT_LISTER}")
            print(f"Attempting to use Brave user data dir: {brave_user_data_dir}")
            print(f"Attempting to use Brave profile: {config.BRAVE_PROFILE_TO_USE_EVENT_LISTER}")
        else:
            print("No Brave profile specified in config. Spinning up a fresh instance.")

    except Exception as e:
        print(f"Error setting up Brave profile path: {e}")
        print("Warning: Proceeding without a specific Brave profile due to error.")
        pass

    if os.path.exists(config.BRAVE_APP_PATH):
        options.binary_location = config.BRAVE_APP_PATH
        print(f"Using Brave binary location: {config.BRAVE_APP_PATH}")
    else:
        print(f"Error: Brave Browser application not found at '{config.BRAVE_APP_PATH}'.")
        return None

    if config.HEADLESS_MODE:
        options.add_argument("--headless")
        print("Event Lister: Running in headless mode (global config).")
    else:
        options.add_argument("--start-maximized") 
        print("Event Lister: Running with visible browser window (global config).")

    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(getattr(config, "PAGE_LOAD_TIMEOUT_EVENT_LISTER", 25))
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver for Brave (event_data.py): {e}")
        print("Ensure ALL Brave instances are completely closed (check Activity Monitor).")
        return None

def extract_trader_count(text):
    if not text: return None
    match = re.search(r'(\d+)\s*traders', text, re.IGNORECASE)
    if match: return int(match.group(1))
    return None

def load_and_prepare_event_file(filepath):
    existing_event_ids = set()
    output_dir = os.path.dirname(filepath)
    if output_dir and not os.path.exists(output_dir):
        try: os.makedirs(output_dir); print(f"Created directory: {output_dir}")
        except OSError as e: print(f"Error creating dir {output_dir}: {e}"); return existing_event_ids 

    file_exists_and_not_empty = os.path.exists(filepath) and os.path.getsize(filepath) > 0
    if not file_exists_and_not_empty:
        try:
            with open(filepath, 'w', encoding='utf-8') as f: f.write(config.FILE_HEADER) 
            print(f"Initialized event file with header: {filepath}")
        except Exception as e: print(f"Error creating/writing header to '{filepath}': {e}")
        return existing_event_ids

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
            if header_line != config.FILE_HEADER.strip():
                print(f"Warning: Header mismatch in '{filepath}'. File may be overwritten or parsing may fail."); 
                # Optionally, decide to re-initialize the file if header is wrong
                # For now, we'll return empty set, implying all scraped events are "new" relative to this file state.
                return existing_event_ids 
            for i, line in enumerate(f, 2): 
                line = line.strip()
                if not line: continue
                parts = line.split(config.DELIMITER)
                if len(parts) == 6: 
                    event_name = parts[config.PSV_COL_EVENT_NAME]
                    started_on = parts[config.PSV_COL_TRADING_STARTED_ON]
                    expires_on = parts[config.PSV_COL_EVENT_EXPIRES_ON]
                    existing_event_ids.add((event_name, started_on, expires_on))
                else: print(f"Warning: Malformed line {i} in '{filepath}': '{line}'")
    except Exception as e: print(f"Error reading existing events file '{filepath}': {e}")
    print(f"Loaded {len(existing_event_ids)} existing event IDs from '{filepath}'.")
    return existing_event_ids

def scrape_and_save_event_details(driver, existing_event_ids):
    initial_event_stubs = [] 
    new_events_added_count = 0
    print(f"Navigating to event listing page: {config.STARTING_URL_EVENT_LISTER}")
    try:
        driver.get(config.STARTING_URL_EVENT_LISTER)
        WebDriverWait(driver, config.PAGE_LOAD_TIMEOUT_EVENT_LISTER).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@class, 'style_home__events__link__')]"))
        )
    except TimeoutException: print(f"Timeout loading listing page: {config.STARTING_URL_EVENT_LISTER}"); return 0
    except Exception as e: print(f"Error loading listing page {config.STARTING_URL_EVENT_LISTER}: {e}"); return 0

    print("Scrolling to load more events...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(config.SCROLL_ATTEMPTS_EVENT_LISTER):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(config.SCROLL_PAUSE_TIME_EVENT_LISTER)
        try:
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height: print("Reached bottom of page."); break
            last_height = new_height
        except Exception as e: print(f"Error during scroll: {e}"); break
    
    print("Finding event cards...")
    event_card_links_xpath = "//a[contains(@class, 'style_home__events__link__')]"
    try:
        event_elements = driver.find_elements(By.XPATH, event_card_links_xpath)
        print(f"Found {len(event_elements)} potential event cards.")
        if not event_elements: return 0
        for element in event_elements:
            name, url_rel, traders = "N/A", None, "N/A"
            try:
                name = element.find_element(By.XPATH, ".//div[contains(@class, 'style_home__event__body__info__title__')]").text.strip()
                url_rel = element.get_attribute('href')
                traders = element.find_element(By.XPATH, ".//div[contains(@class, 'style_home__event__header__text__')]").text.strip()
                if name != "N/A" and url_rel:
                    abs_url = config.BASE_URL_PROBO + url_rel if not url_rel.startswith("http") else url_rel
                    initial_event_stubs.append({"name": name, "url": abs_url, "traders_text": traders})
            except Exception: pass
    except Exception as e: print(f"Error finding event cards: {e}")

    print(f"\nCollected {len(initial_event_stubs)} event stubs. Visiting detail pages...")
    for i, event_stub in enumerate(initial_event_stubs):
        print(f"\nProcessing event {i+1}/{len(initial_event_stubs)}: {event_stub['name']}")
        started_on, expires_on = "N/A", "N/A"
        try:
            driver.get(event_stub['url'])
            WebDriverWait(driver, config.ELEMENT_VISIBILITY_TIMEOUT_EVENT_LISTER).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'style_event__overview__title__')]"))
            )
            try:
                started_on = driver.find_element(By.XPATH, "//div[normalize-space()='Trading started on']/following-sibling::div").text.strip()
            except Exception: print(f"    'Trading started on' not found.")
            try:
                expires_on = driver.find_element(By.XPATH, "//div[normalize-space()='Event expires on']/following-sibling::div").text.strip()
            except Exception: print(f"    'Event expires on' not found.")
        except Exception as e: print(f"  Error visiting/parsing detail page {event_stub['url']}: {e}"); continue
        
        event_id_tuple = (event_stub["name"], started_on, expires_on)
        if event_id_tuple in existing_event_ids:
            print(f"    Skipping duplicate: {event_stub['name']}")
            continue

        trader_count = str(extract_trader_count(event_stub["traders_text"]) or event_stub["traders_text"])
        parts = [s.replace(config.DELIMITER, " ").replace("\n", " ") for s in [started_on, expires_on, event_stub["name"], trader_count, event_stub["url"]]]
        parts.append("0") # Record column default to "0"
        psv_line = config.DELIMITER.join(parts)
        try:
            with open(config.EVENTS_FILE_PATH, 'a', encoding='utf-8') as f: f.write(psv_line + "\n")
            existing_event_ids.add(event_id_tuple) 
            new_events_added_count += 1
            print(f"    Appended: {psv_line}")
        except Exception as e: print(f"    Error writing to file: {e}")
        time.sleep(0.5) 
    return new_events_added_count

if __name__ == "__main__":
    print("Starting Probo Event Lister and Detail Scraper...")
    existing_ids = load_and_prepare_event_file(config.EVENTS_FILE_PATH)
    driver = setup_driver_for_event_lister() # Uses global headless flag from config
    if driver:
        count = 0
        try:
            count = scrape_and_save_event_details(driver, existing_ids)
            print(f"\nFinished. Added {count} new events to '{config.EVENTS_FILE_PATH}'.")
        except KeyboardInterrupt: print("\nProcess stopped by user.")
        except Exception as e: print(f"Main error (event_data.py): {e}")
        finally:
            print("Closing Brave web driver (event_data.py).")
            if driver: driver.quit()
    else: print("Failed to initialize Brave web driver for event_data.py.")
