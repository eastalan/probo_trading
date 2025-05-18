# market_data_worker.py
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import datetime
import os
import json
import re
import tempfile # For temporary profile directories
import shutil   # For cleaning up directories
# NEW IMPORTS for WebDriverWait
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import datetime
import os
import json
import re
import tempfile
import shutil

import config

import config # Your configuration file

def slugify(text):
    if not text: return "unknown_event"
    text = str(text).lower() # Ensure text is string
    text = re.sub(r'\s+', '_', text)
    text = re.sub(r'[^\w_.-]', '', text)
    text = text[:100] # Truncate to a max length
    if not text: return f"event_{int(time.time())}" # Fallback if slug becomes empty
    return text

def cleanup_temp_profile_dir(profile_path, worker_pid_for_log="N/A"):
    if profile_path and os.path.exists(profile_path):
        try:
            shutil.rmtree(profile_path)
            print(f"[Worker PID: {worker_pid_for_log}] Successfully cleaned up temp profile: {profile_path}")
        except Exception as e:
            print(f"[Worker PID: {worker_pid_for_log}] Error cleaning up temp profile {profile_path}: {e}")

def setup_worker_driver(assigned_profile_name=None, worker_pid=None):
    effective_pid = worker_pid if worker_pid else os.getpid()
    options = Options()
    temp_profile_dir_path = None # Will store path if a temp profile is created

    print(f"[Worker PID: {effective_pid}] Initializing WebDriver. Requested profile: '{assigned_profile_name}', Headless: {config.HEADLESS_MODE}")

    # Attempt to use a specific existing profile if one is provided and isn't the "FRESH" signal
    if assigned_profile_name and assigned_profile_name != "FRESH_PROFILE_REQUEST":
        print(f"[Worker PID: {effective_pid}] Attempting to use existing Brave profile: '{assigned_profile_name}'")
        try:
            home_directory = os.path.expanduser("~")
            # This path structure is common for Brave, adjust if yours differs significantly
            # On Windows, it's usually in %LOCALAPPDATA%\BraveSoftware\Brave-Browser\User Data
            # On Linux, ~/.config/BraveSoftware/Brave-Browser/
            # On macOS, ~/Library/Application Support/BraveSoftware/Brave-Browser/
            if "Windows" in os.uname().sysname: # A bit simplistic, platform module is better
                 brave_user_data_dir_base = os.path.join(home_directory, "AppData", "Local", "BraveSoftware")
            elif "Linux" in os.uname().sysname:
                 brave_user_data_dir_base = os.path.join(home_directory, ".config", "BraveSoftware")
            else: # macOS default
                 brave_user_data_dir_base = os.path.join(home_directory, "Library/Application Support/BraveSoftware")

            brave_user_data_dir = os.path.join(brave_user_data_dir_base, "Brave-Browser")

            if not os.path.isdir(brave_user_data_dir): # Base user data for Brave (contains all profiles)
                print(f"WORKER_ERROR (PID {effective_pid}): Brave user data directory not found: {brave_user_data_dir}")
                print(f"WORKER_WARNING (PID {effective_pid}): Will fall back to creating a fresh temporary profile.")
                assigned_profile_name = "FRESH_PROFILE_REQUEST" # Force fresh profile
            else:
                # For Brave, profile directories are often "Default", "Profile 1", "Profile 2", etc.
                # The `user-data-dir` should point to the parent of these (e.g., Brave-Browser folder)
                # and `profile-directory` is the specific profile folder name.
                profile_path_to_check = os.path.join(brave_user_data_dir, assigned_profile_name)
                if not os.path.isdir(profile_path_to_check):
                    print(f"WORKER_ERROR (PID {effective_pid}): Assigned Brave profile folder '{assigned_profile_name}' not found at '{profile_path_to_check}'.")
                    print(f"WORKER_WARNING (PID {effective_pid}): Will fall back to creating a fresh temporary profile.")
                    assigned_profile_name = "FRESH_PROFILE_REQUEST" # Force fresh profile
                else:
                    options.add_argument(f"--user-data-dir={brave_user_data_dir}") # Path to "User Data" like dir
                    options.add_argument(f"--profile-directory={assigned_profile_name}") # e.g., "Profile 1"
                    print(f"[Worker PID: {effective_pid}] Configured to use existing profile: '{assigned_profile_name}' from '{brave_user_data_dir}'")
        except Exception as e:
            print(f"WORKER_ERROR (PID {effective_pid}): Error setting up assigned profile '{assigned_profile_name}': {e}")
            print(f"WORKER_WARNING (PID {effective_pid}): Will fall back to creating a fresh temporary profile.")
            assigned_profile_name = "FRESH_PROFILE_REQUEST" # Force fresh profile

    # Create a fresh, temporary profile if requested or if existing profile setup failed
    if assigned_profile_name == "FRESH_PROFILE_REQUEST" or assigned_profile_name is None:
        try:
            temp_profile_dir_path = tempfile.mkdtemp(prefix=f"brave_worker_{effective_pid}_")
            options.add_argument(f"--user-data-dir={temp_profile_dir_path}")
            # When using a unique user-data-dir, --profile-directory is not strictly needed (it'll use a default within that dir)
            # Using --incognito here would be redundant and might conflict.
            print(f"[Worker PID: {effective_pid}] Initializing with a fresh temporary profile at: {temp_profile_dir_path}")
        except Exception as e:
            print(f"WORKER_CRITICAL (PID {effective_pid}): Could not create temporary profile directory: {e}")
            print(f"WORKER_WARNING (PID {effective_pid}): Attempting to fall back to incognito mode (less isolated).")
            options.add_argument("--incognito") # Fallback, less ideal for full isolation.
            temp_profile_dir_path = None # No specific dir to clean up if incognito is used.

    if not os.path.exists(config.BRAVE_APP_PATH):
        print(f"WORKER_CRITICAL (PID {effective_pid}): Brave Browser application not found at '{config.BRAVE_APP_PATH}'. Cannot start worker.")
        if temp_profile_dir_path: cleanup_temp_profile_dir(temp_profile_dir_path, effective_pid)
        return None, None
    options.binary_location = config.BRAVE_APP_PATH

    if config.HEADLESS_MODE:
        options.add_argument("--headless=new") # Use "new" headless mode
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-popup-blocking")

    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox") # Often necessary in Docker/CI environments
    options.add_argument("--window-size=1366x768") # Common window size
    options.add_argument("--disable-dev-shm-usage") # Overcome limited resource problems in Docker
    options.add_argument("--log-level=3") # Reduce console noise from Chrome/ChromeDriver
    options.add_argument("--silent")
    options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36") # Generic modern UA
    options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled') # Try to hide automation

    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(config.INITIAL_PAGE_LOAD_WAIT_SECONDS_WORKER + 5) # Generous timeout
        print(f"[Worker PID: {effective_pid}] WebDriver initialized successfully. Profile in use: {temp_profile_dir_path or assigned_profile_name or 'Incognito'}")
        return driver, temp_profile_dir_path # Return driver and path for cleanup
    except WebDriverException as e:
        print(f"WORKER_CRITICAL (PID {effective_pid}): WebDriverException during initialization: {e}")
        if "user data directory is already in use" in str(e).lower():
            if assigned_profile_name and assigned_profile_name != "FRESH_PROFILE_REQUEST":
                 print(f"WORKER_HINT (PID {effective_pid}): The existing profile '{assigned_profile_name}' might be locked by another Brave instance.")
            elif temp_profile_dir_path: # Should be rare with unique temp dirs
                 print(f"WORKER_HINT (PID {effective_pid}): The temporary profile directory '{temp_profile_dir_path}' is unexpectedly locked.")
        if temp_profile_dir_path: cleanup_temp_profile_dir(temp_profile_dir_path, effective_pid)
        return None, None
    except Exception as e:
        print(f"WORKER_CRITICAL (PID {effective_pid}): Generic error setting up WebDriver: {e}")
        if temp_profile_dir_path: cleanup_temp_profile_dir(temp_profile_dir_path, effective_pid)
        return None, None


def get_order_book_side_data_worker(driver, prices_xpath, quantities_xpath, side_name, worker_pid):
    side_book_data = []
    try:
        price_elements = driver.find_elements(By.XPATH, prices_xpath)
        quantity_elements = driver.find_elements(By.XPATH, quantities_xpath)

        if not price_elements:
            # print(f"[Worker PID: {worker_pid}] No price elements found for {side_name} side.")
            return [] # No orders, not necessarily an error

        num_entries = min(len(price_elements), len(quantity_elements))
        if len(price_elements) != len(quantity_elements):
            print(f"[Worker PID: {worker_pid}] Warning: Mismatch in count of price ({len(price_elements)}) and quantity ({len(quantity_elements)}) elements for {side_name} side.")

        for i in range(num_entries):
            try:
                price_text = price_elements[i].text.strip().replace('₹', '')
                # For quantities like "1.5K Shares", "500 Shares"
                qty_text_raw = quantity_elements[i].text.strip()
                qty_text_numeric = re.match(r'([\d.]+)', qty_text_raw) # Get numeric part
                qty_value = "0"
                if qty_text_numeric:
                    qty_value = qty_text_numeric.group(1)
                    if 'K' in qty_text_raw.upper():
                        qty_value = str(float(qty_value) * 1000)
                    # Add M for million if necessary

                if price_text and qty_value:
                    side_book_data.append({"price": float(price_text), "qty": int(float(qty_value))}) # Qty can be float then int
            except ValueError as ve:
                # print(f"[Worker PID: {worker_pid}] ValueError parsing for {side_name} entry {i}: Price='{price_elements[i].text}', QtyRaw='{quantity_elements[i].text}'. Error: {ve}")
                continue
            except Exception as e_entry:
                # print(f"[Worker PID: {worker_pid}] Exception parsing {side_name} entry {i}: {e_entry}")
                continue
        return side_book_data
    except (NoSuchElementException, TimeoutException):
        print(f"[Worker PID: {worker_pid}] Error finding elements for {side_name} order book (NoSuchElement/Timeout).")
        return None # Indicates a fetch error for this side
    except Exception as e:
        print(f"[Worker PID: {worker_pid}] Unexpected error in get_order_book_side_data_worker for {side_name}: {e}")
        return None # Indicates a generic error


def record_market_data_for_event(event_name, event_url, trading_started_on, event_expires_on,
                                 profile_request_info):
    worker_pid = os.getpid()
    profile_display_name = profile_request_info if profile_request_info and profile_request_info != "FRESH_PROFILE_REQUEST" else "Fresh/Temp"
    print(f"[Worker PID: {worker_pid}] Starting: '{event_name[:60]}' URL: {event_url} Profile: {profile_display_name} Headless: {config.HEADLESS_MODE}")

    driver, temp_profile_path = setup_worker_driver(
        assigned_profile_name=profile_request_info,
        worker_pid=worker_pid
    )

    if not driver:
        print(f"[Worker PID: {worker_pid}] Failed WebDriver init for '{event_name}'. Exiting worker.")
        return

    event_slug = slugify(event_name)
    if not event_slug or event_slug == "unknown_event":
        url_parts = event_url.split('/')
        event_slug = slugify(url_parts[-1] if url_parts[-1] else (url_parts[-2] if len(url_parts) > 1 else f"event_{worker_pid}"))

    output_filename = f"{event_slug}_market_data.jsonl"
    output_filepath = os.path.join(config.MARKET_DATA_BASE_DIR, output_filename)

    try:
        if not os.path.exists(config.MARKET_DATA_BASE_DIR):
            os.makedirs(config.MARKET_DATA_BASE_DIR)
    except OSError as e:
        print(f"[Worker PID: {worker_pid}] CRITICAL: Error creating dir {config.MARKET_DATA_BASE_DIR}: {e}. Exiting.")
        driver.quit(); cleanup_temp_profile_dir(temp_profile_path, worker_pid); return

    file_mode = 'a' if os.path.exists(output_filepath) and os.path.getsize(output_filepath) > 0 else 'w'
    # print(f"[Worker PID: {worker_pid}] File mode for {output_filepath}: {file_mode}") # Less verbose

    try:
        with open(output_filepath, file_mode, encoding='utf-8') as f:
            if file_mode == 'w':
                f.write(f"# Event: {event_name} ({event_url})\n# PID: {worker_pid}, Profile: {temp_profile_path or profile_display_name}\n")
                f.write(f"# Started: {trading_started_on}, Expires: {event_expires_on}\n# Session Start: {datetime.datetime.now().isoformat()}\n")
    except Exception as e:
        print(f"[Worker PID: {worker_pid}] CRITICAL: Error opening/writing header to {output_filepath}: {e}. Exiting.")
        driver.quit(); cleanup_temp_profile_dir(temp_profile_path, worker_pid); return

    page_loaded_successfully = False
    for attempt in range(3): # Max 3 load attempts
        try:
            print(f"[Worker PID: {worker_pid}] Navigating to event page: {event_url} (Attempt {attempt + 1})")
            driver.get(event_url)
            # Basic check for page title or a known static element after get()
            WebDriverWait(driver, config.INITIAL_PAGE_LOAD_WAIT_SECONDS_WORKER).until(
                EC.presence_of_element_located((By.TAG_NAME, "body")) # Wait for body to be present
            )
            page_loaded_successfully = True
            print(f"[Worker PID: {worker_pid}] Page navigation complete for '{event_name}'.")
            break
        except TimeoutException:
            print(f"[Worker PID: {worker_pid}] Timeout loading page '{event_name}' (Attempt {attempt + 1}).")
            if attempt == 2: driver.quit(); cleanup_temp_profile_dir(temp_profile_path, worker_pid); return
            time.sleep(3)
        except Exception as e:
            print(f"[Worker PID: {worker_pid}] Error loading page '{event_name}' (Attempt {attempt + 1}): {e}")
            if attempt == 2: driver.quit(); cleanup_temp_profile_dir(temp_profile_path, worker_pid); return
            time.sleep(3)

    if not page_loaded_successfully:
        print(f"[Worker PID: {worker_pid}] Failed to load page '{event_name}' after multiple attempts. Exiting.")
        # driver.quit() and cleanup_temp_profile_dir already called in loop if failed
        return

    # --- NEW: LOGIN/SIGNUP OVERLAY CHECK ---
    if config.CHECK_FOR_LOGIN_OVERLAY and config.XPATH_LOGIN_SIGNUP_OVERLAY:
        print(f"[Worker PID: {worker_pid}] Checking for login/signup overlay (max {config.WAIT_FOR_LOGIN_OVERLAY_SECONDS}s)...")
        overlay_disappeared = False
        try:
            # Determine which element to wait for invisibility
            xpath_to_wait_for = config.XPATH_LOGIN_SIGNUP_OVERLAY

            # First, quickly check if the overlay (or its blocking element) is even there and displayed
            # Use a very short timeout for this initial check to avoid waiting if it's not there
            initial_check_wait = WebDriverWait(driver, 3) # 3 seconds for initial check
            try:
                overlay_element = initial_check_wait.until(EC.presence_of_element_located((By.XPATH, xpath_to_wait_for)))
                if overlay_element and overlay_element.is_displayed():
                    print(f"[Worker PID: {worker_pid}] Login/signup element '{xpath_to_wait_for}' detected and visible. Waiting for it to disappear...")
                    # Now wait for the longer duration for it to become invisible
                    WebDriverWait(driver, config.WAIT_FOR_LOGIN_OVERLAY_SECONDS).until(
                        EC.invisibility_of_element_located((By.XPATH, xpath_to_wait_for))
                    )
                    overlay_disappeared = True
                    print(f"[Worker PID: {worker_pid}] Login/signup element disappeared.")
                else:
                    # Element present but not displayed, or check timed out (meaning not obviously present)
                    overlay_disappeared = True # Treat as if it's not an issue
                    print(f"[Worker PID: {worker_pid}] Login/signup element not initially visible or not found quickly. Proceeding.")
            except TimeoutException:
                # This means the element was not found/visible within the short initial_check_wait
                overlay_disappeared = True # Treat as if it's not an issue
                print(f"[Worker PID: {worker_pid}] Login/signup element not detected initially. Proceeding.")

        except TimeoutException: # This catches timeout from the main WebDriverWait for invisibility
            print(f"[Worker PID: {worker_pid}] WARNING: Login/signup element '{xpath_to_wait_for}' did NOT disappear within {config.WAIT_FOR_LOGIN_OVERLAY_SECONDS}s for '{event_name}'. Polling will proceed but may be affected.")
        except Exception as e_overlay:
            print(f"[Worker PID: {worker_pid}] WARNING: An error occurred while checking for login/signup overlay for '{event_name}': {e_overlay}. Proceeding.")

        if not overlay_disappeared and not (config.XPATH_LOGIN_BLOCKING_ELEMENT and not driver.find_elements(By.XPATH, config.XPATH_LOGIN_BLOCKING_ELEMENT)): # Double check if it timed out but element is gone
             print(f"[Worker PID: {worker_pid}] Overlay check indicated potential issue, but proceeding to poll data for '{event_name}'.")
    # --- END: LOGIN/SIGNUP OVERLAY CHECK ---

    print(f"[Worker PID: {worker_pid}] Page '{event_name}' ready. Starting data polling loop.")
    consecutive_fetch_errors = 0
    max_consecutive_fetch_errors = 10

    try:
        last_refresh_time = time.monotonic()  # Track last refresh for page
        while True:
            loop_start_time = time.monotonic()
            # Format timestamp as "YYYY-MM-DD HH:MM:SS.ssssss" (no 'T')
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            try:
                _ = driver.title
            except WebDriverException as e_wd_responsive:
                print(f"[Worker PID: {worker_pid}] WebDriver unresponsive for '{event_name}': {e_wd_responsive}. Exiting.")
                break

            yes_book = get_order_book_side_data_worker(driver, config.XPATH_YES_ALL_PRICES, config.XPATH_YES_ALL_QTYS, "YES", worker_pid)
            no_book = get_order_book_side_data_worker(driver, config.XPATH_NO_ALL_PRICES, config.XPATH_NO_ALL_QTYS, "NO", worker_pid)
            record = {"timestamp": timestamp, "event_name": event_name, "order_book": {}}
            data_fetched_this_cycle = False

            if yes_book is not None: record["order_book"]["yes"] = yes_book; data_fetched_this_cycle = True if yes_book else data_fetched_this_cycle
            else: record["order_book"]["yes_error"] = "FetchError"
            if no_book is not None: record["order_book"]["no"] = no_book; data_fetched_this_cycle = True if no_book else data_fetched_this_cycle
            else: record["order_book"]["no_error"] = "FetchError"

            if data_fetched_this_cycle or "FetchError" in str(record["order_book"]):
                try:
                    with open(output_filepath, 'a', encoding='utf-8') as f: f.write(json.dumps(record) + "\n")
                    consecutive_fetch_errors = 0
                except Exception as e_write:
                    print(f"[Worker PID: {worker_pid}] Error writing data to {output_filepath}: {e_write}"); consecutive_fetch_errors += 1
            else:
                consecutive_fetch_errors += 1

            # --- Refresh the page every 4 minutes (240 seconds) ---
            if time.monotonic() - last_refresh_time >= 240:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] [Worker PID: {worker_pid}] Refreshing page to keep data fresh...")
                try:
                    driver.refresh()
                    print(f"[Worker PID: {worker_pid}] Page refreshed successfully.")
                except Exception as e:
                    print(f"[Worker PID: {worker_pid}] Error refreshing page: {e}")
                last_refresh_time = time.monotonic()

            if consecutive_fetch_errors >= max_consecutive_fetch_errors:
                print(f"[Worker PID: {worker_pid}] Max ({max_consecutive_fetch_errors}) consecutive fetch errors for '{event_name}'. Stopping worker.")
                time.sleep(15)
                break
            processing_time = time.monotonic() - loop_start_time
            sleep_duration = config.WORKER_POLLING_INTERVAL_SECONDS - processing_time
            if sleep_duration > 0: time.sleep(sleep_duration)

    except KeyboardInterrupt: print(f"[Worker PID: {worker_pid}] KeyboardInterrupt for '{event_name}'.")
    except WebDriverException as e_wd: print(f"[Worker PID: {worker_pid}] WebDriverException in main loop for '{event_name}': {e_wd}.")
    except Exception as e_main: print(f"[Worker PID: {worker_pid}] Critical error in main loop for '{event_name}': {e_main}")
    finally:
        print(f"[Worker PID: {worker_pid}] Stopping worker for '{event_name}'.")
        if driver:
            try: driver.quit()
            except Exception: pass # ignore errors during quit
        if temp_profile_path: cleanup_temp_profile_dir(temp_profile_path, worker_pid)
        print(f"[Worker PID: {worker_pid}] Worker for '{event_name}' finished.")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Market Data Worker - Monitors a single event.")
    parser.add_argument("event_name_arg", help="Name of the event.")
    parser.add_argument("event_url_arg", help="URL of the event page.")
    parser.add_argument("start_date_arg", help="Event start date (for header).", default="N/A")
    parser.add_argument("expiry_date_arg", help="Event expiry date (for header).", default="N/A")
    parser.add_argument("--profile", help="Specific Brave profile or 'FRESH_PROFILE_REQUEST'.", default="FRESH_PROFILE_REQUEST")
    cli_args = parser.parse_args()
    print(f"--- market_data_worker.py directly (PID: {os.getpid()}) Name='{cli_args.event_name_arg}', URL='{cli_args.event_url_arg}', Profile='{cli_args.profile}', Headless={config.HEADLESS_MODE} ---")
    record_market_data_for_event(cli_args.event_name_arg, cli_args.event_url_arg, cli_args.start_date_arg, cli_args.expiry_date_arg, cli_args.profile)
    print(f"--- market_data_worker.py direct execution finished ---")