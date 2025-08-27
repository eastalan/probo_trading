# probo_monitor.py (Standalone single-event monitor)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import pdb
import datetime  # Ensure datetime is imported
import os
import json
import re

import config  # Import global config


def slugify_filename(text):
    if not text: return "unknown_event"
    text = text.lower()
    text = re.sub(r'\s+', '_', text)
    text = re.sub(r'[^\w_.-]', '', text)
    return text[:100]


def setup_standalone_driver():
    options = Options()
    try:
        home_directory = os.path.expanduser("~")
        brave_user_data_dir_base = os.path.join(home_directory, "Library/Application Support/BraveSoftware/")
        brave_user_data_dir = os.path.join(brave_user_data_dir_base, "Brave-Browser/")
        if not os.path.isdir(brave_user_data_dir):
            print(f"Error: Brave user data directory not found: {brave_user_data_dir}")
        else:
            profile_path_to_check = os.path.join(brave_user_data_dir, config.BRAVE_PROFILE_TO_USE_STANDALONE)
            if not os.path.isdir(profile_path_to_check):
                print(f"Error: Brave profile '{config.BRAVE_PROFILE_TO_USE_STANDALONE}' not found.")
            else:
                options.add_argument(f"user-data-dir={brave_user_data_dir}")
                options.add_argument(f"profile-directory={config.BRAVE_PROFILE_TO_USE_STANDALONE}")
                print(f"Standalone Monitor: Attempting to use Brave profile: {config.BRAVE_PROFILE_TO_USE_STANDALONE}")
    except Exception as e:
        print(f"Standalone Monitor: Error setting up Brave profile path: {e}")
        pass

    if os.path.exists(config.BRAVE_APP_PATH):
        options.binary_location = config.BRAVE_APP_PATH
    else:
        print(f"Error: Brave Browser application not found at '{config.BRAVE_APP_PATH}'.")
        return None

    if config.HEADLESS_MODE:
        options.add_argument("--headless=new")
        print("Standalone Monitor: Running in headless mode (global config).")
    else:
        options.add_argument("--start-maximized")
        print("Standalone Monitor: Running with visible browser window (global config).")

    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--remote-debugging-port=9222") # Optional: can sometimes cause issues if port is in use

    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(config.STANDALONE_INITIAL_LOAD_SECONDS + 5)
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver for Brave (standalone): {e}")
        return None


def get_order_book_data_standalone(driver):
    yes_book_list, no_book_list = [], []
    try:
        yes_prices = driver.find_elements(By.XPATH, config.XPATH_YES_ALL_PRICES)
        yes_qtys = driver.find_elements(By.XPATH, config.XPATH_YES_ALL_QTYS)
        for p, q in zip(yes_prices, yes_qtys):
            try:
                yes_book_list.append(
                    {"price": float(p.text.strip().replace('₹', '')), "qty": int(q.text.strip().replace(',', ''))})
            except:
                pass
    except:
        pass
    try:
        no_prices = driver.find_elements(By.XPATH, config.XPATH_NO_ALL_PRICES)
        no_qtys = driver.find_elements(By.XPATH, config.XPATH_NO_ALL_QTYS)
        for p, q in zip(no_prices, no_qtys):
            try:
                no_book_list.append(
                    {"price": float(p.text.strip().replace('₹', '')), "qty": int(q.text.strip().replace(',', ''))})
            except:
                pass
    except:
        pass
    return {"yes": yes_book_list, "no": no_book_list}


def is_new_data_different(new_data, last_data):
    """
    Compares the new_data dict with last_data dict.
    Returns True if different, False if same.
    """
    if last_data is None:
        return True
    # Compare the order_book section only (ignoring timestamp)
    return new_data.get("order_book") != last_data.get("order_book")


def monitor_single_event(driver):
    event_url = config.STANDALONE_EVENT_URL_TO_MONITOR

    # --- NEW: Add current date subfolder ---
    current_date_str = datetime.datetime.now().strftime("%Y%m%d")
    output_dir = os.path.join(config.MARKET_DATA_BASE_DIR, current_date_str)

    event_name_for_header = "Unknown Event"
    try:
        url_parts = event_url.split('/')
        slug_part = url_parts[-1].split('?')[0]
        event_name_for_header = slug_part.replace('-', ' ').replace('_', ' ').title()
    except:
        pass

    slug = slugify_filename(event_name_for_header)
    output_file = f"{slug}_market_data.jsonl"
    output_filepath = os.path.join(output_dir, output_file)

    print(f"\nStarting to monitor single event: {event_name_for_header} ({event_url})")
    print(f"Logging data to: {output_filepath}")

    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")
        except OSError as e:
            print(f"Error creating dir {output_dir}: {e}. Exiting.")
            return

    file_exists_and_has_content = os.path.exists(output_filepath) and os.path.getsize(output_filepath) > 0
    file_mode = 'a'
    if not file_exists_and_has_content:
        file_mode = 'w'
        print(f"Creating new data file with header: {output_filepath}")
    else:
        print(f"Appending to existing data file: {output_filepath}")

    try:
        if file_mode == 'w':
            with open(output_filepath, file_mode, encoding='utf-8') as f:
                f.write(f"# Market data for event: {event_name_for_header} ({event_url})\n")
                f.write(f"# Monitoring session started: {datetime.datetime.now()}\n")
    except Exception as e:
        print(f"Error opening/writing header to {output_filepath}: {e}")
        return

    try:
        driver.get(event_url)
        print(f"Waiting for initial page load ({config.STANDALONE_INITIAL_LOAD_SECONDS} seconds)...")
        time.sleep(config.STANDALONE_INITIAL_LOAD_SECONDS)
        print("Initial load complete. Starting polling.")
    except Exception as e:
        print(f"Error loading page {event_url}: {e}")
        return

    kick_off_datetime_obj = None
    if config.STANDALONE_KICK_OFF_DATETIME_STR:
        try:
            kick_off_datetime_obj = datetime.datetime.strptime(
                config.STANDALONE_KICK_OFF_DATETIME_STR, "%Y-%m-%d %H:%M:%S"
            )
            print(f"Kick-off datetime set to: {kick_off_datetime_obj.strftime('%Y-%m-%d %H:%M:%S')}")
        except ValueError:
            print(
                f"Error: Invalid STANDALONE_KICK_OFF_DATETIME_STR format '{config.STANDALONE_KICK_OFF_DATETIME_STR}'. Expected YYYY-MM-DD HH:MM:SS. Using normal polling.")
        except Exception as e:
            print(f"Error parsing kick-off datetime: {e}. Using normal polling.")
    else:
        print("No kick-off datetime specified. Using normal polling interval.")

    last_refresh_time = time.monotonic()
    REFRESH_INTERVAL_SECONDS = 2.5 * 60  # 4 minutes

    last_dumped_data = None

    try:
        while True:
            loop_start_time = time.monotonic()
            current_dt_obj = datetime.datetime.now()
            timestamp = current_dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f")

            # Refresh page every 4 minutes
            if time.monotonic() - last_refresh_time > REFRESH_INTERVAL_SECONDS:
                print(f"[{timestamp}] Refreshing page after 4 minutes...")
                try:
                    driver.refresh()
                    print(f"[{timestamp}] Page refreshed.")
                except Exception as e:
                    print(f"[{timestamp}] Error refreshing page: {e}")
                last_refresh_time = time.monotonic()

            order_book = get_order_book_data_standalone(driver)
            record = {"timestamp": timestamp, "order_book": order_book}
            if is_new_data_different(record, last_dumped_data):
                with open(output_filepath, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(record) + "\n")
                last_dumped_data = record

            current_polling_interval = config.STANDALONE_POLLING_INTERVAL
            if kick_off_datetime_obj:
                if current_dt_obj < kick_off_datetime_obj:
                    current_polling_interval = config.STANDALONE_POLLING_INTERVAL * config.STANDALONE_POLLING_INTERVAL_PRE_KICKOFF_FACTOR

            proc_time = time.monotonic() - loop_start_time
            sleep_dur = current_polling_interval - proc_time
            if sleep_dur > 0:
                time.sleep(sleep_dur)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"Critical error in polling loop: {e}")
    finally:
        print("Finished monitoring single event.")


if __name__ == "__main__":
    print("Setting up Brave web driver for single event monitoring...")
    driver = setup_standalone_driver()
    if driver:
        try:
            monitor_single_event(driver)
        except Exception as e:
            print(f"Error during single event monitoring: {e}")
        finally:
            print("Closing Brave web driver (standalone).")
            if driver: driver.quit()
    else:
        print("Failed to initialize Brave web driver for standalone monitoring.")
