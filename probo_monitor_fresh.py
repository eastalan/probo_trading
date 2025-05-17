# probo_monitor.py (Standalone single-event monitor)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
# import pdb # Commented out pdb, uncomment if you need it for debugging
import datetime
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

    # --- REMOVED: Logic for loading a specific Brave profile and user-data-dir ---
    # By not specifying user-data-dir or profile-directory, Selenium will
    # create a fresh, temporary profile for each instance.
    print("Standalone Monitor: Initializing Brave with a fresh, temporary profile.")

    # Set the Brave Browser executable location
    # Ensure config.BRAVE_APP_PATH is correctly set in your config.py
    if hasattr(config, 'BRAVE_APP_PATH') and config.BRAVE_APP_PATH and os.path.exists(config.BRAVE_APP_PATH):
        options.binary_location = config.BRAVE_APP_PATH
    else:
        brave_path_in_config = getattr(config, 'BRAVE_APP_PATH', "'Not Set in config.py'")
        print(f"Error: Brave Browser application path `BRAVE_APP_PATH` ({brave_path_in_config}) not found or not set correctly in config.py.")
        return None

    # Apply other configurations (headless mode, user-agent, etc.)
    if hasattr(config, 'HEADLESS_MODE') and config.HEADLESS_MODE:
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
    # options.add_argument("--remote-debugging-port=9222") # Optional, ensure unique ports if running multiple instances and uncommenting

    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        # Use getattr for config values with a default to prevent AttributeError if not set
        initial_load_seconds = getattr(config, 'STANDALONE_INITIAL_LOAD_SECONDS', 30) # Default to 30 if not in config
        driver.set_page_load_timeout(initial_load_seconds + 5)
        print(f"Standalone Monitor: WebDriver initialized for Brave. Page load timeout set to {initial_load_seconds + 5}s.")
        return driver
    except WebDriverException as e:
        print(f"Error setting up WebDriver for Brave (standalone): {e}")
        if "cannot find brave binary" in str(e).lower():
             print(f"Hint: The path '{options.binary_location}' set for BRAVE_APP_PATH in config.py might be incorrect.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during WebDriver setup (standalone): {e}")
        return None


def get_order_book_data_standalone(driver):
    yes_book_list, no_book_list = [], []
    try:
        # Ensure config attributes exist before accessing
        xpath_yes_prices = getattr(config, 'XPATH_YES_ALL_PRICES', None)
        xpath_yes_qtys = getattr(config, 'XPATH_YES_ALL_QTYS', None)
        xpath_no_prices = getattr(config, 'XPATH_NO_ALL_PRICES', None)
        xpath_no_qtys = getattr(config, 'XPATH_NO_ALL_QTYS', None)

        if xpath_yes_prices:
            yes_prices = driver.find_elements(By.XPATH, xpath_yes_prices)
            yes_qtys = driver.find_elements(By.XPATH, xpath_yes_qtys) if xpath_yes_qtys else [None] * len(yes_prices) # Handle if QTY XPATH missing
            for p, q_el in zip(yes_prices, yes_qtys):
                try:
                    price_text = p.text.strip().replace('₹', '')
                    qty_text = q_el.text.strip().replace(',', '') if q_el else "0" # Default qty if element missing
                    if price_text: # Price is mandatory
                        yes_book_list.append({
                            "price": float(price_text), 
                            "qty": int(qty_text) if qty_text else 0
                        })
                except (ValueError, AttributeError) as e:
                    # print(f"Warning: Could not parse yes price/qty: Price='{p.text if p else 'N/A'}', Qty='{q_el.text if q_el else 'N/A'}'. Error: {e}")
                    pass # Silently skip problematic entries
        else:
            print("Warning: XPATH_YES_ALL_PRICES not defined in config.")

        if xpath_no_prices:
            no_prices = driver.find_elements(By.XPATH, xpath_no_prices)
            no_qtys = driver.find_elements(By.XPATH, xpath_no_qtys) if xpath_no_qtys else [None] * len(no_prices)
            for p, q_el in zip(no_prices, no_qtys):
                try:
                    price_text = p.text.strip().replace('₹', '')
                    qty_text = q_el.text.strip().replace(',', '') if q_el else "0"
                    if price_text:
                        no_book_list.append({
                            "price": float(price_text), 
                            "qty": int(qty_text) if qty_text else 0
                        })
                except (ValueError, AttributeError) as e:
                    # print(f"Warning: Could not parse no price/qty: Price='{p.text if p else 'N/A'}', Qty='{q_el.text if q_el else 'N/A'}'. Error: {e}")
                    pass
        else:
            print("Warning: XPATH_NO_ALL_PRICES not defined in config.")

    except NoSuchElementException:
        # This might indicate that the page structure for order books isn't present or XPaths are incorrect
        # print("Warning: Order book elements not found (NoSuchElementException). Page might not have loaded correctly or XPaths are invalid.")
        pass
    except Exception as e:
        # print(f"An error occurred in get_order_book_data_standalone: {e}")
        pass # Catch all other potential errors during data extraction
    return {"yes": yes_book_list, "no": no_book_list}


def monitor_single_event(driver):
    event_url = getattr(config, 'STANDALONE_EVENT_URL_TO_MONITOR', None)
    if not event_url:
        print("Error: STANDALONE_EVENT_URL_TO_MONITOR not set in config.py. Exiting.")
        return
        
    output_dir = getattr(config, 'MARKET_DATA_BASE_DIR', 'market_data_standalone_default') # Default if not in config

    event_name_for_header = "Unknown Event"
    try:
        if event_url: # Ensure event_url is not None
            url_parts = event_url.split('/')
            slug_part = url_parts[-1].split('?')[0] if url_parts else "event"
            event_name_for_header = slug_part.replace('-', ' ').replace('_', ' ').title() if slug_part else "Event"
    except:
        pass # Keep "Unknown Event" on error

    slug = slugify_filename(event_name_for_header)
    output_file = f"{slug}_market_data.jsonl"
    output_filepath = os.path.join(output_dir, output_file)

    print(f"\nStarting to monitor single event: {event_name_for_header} ({event_url})")
    print(f"Logging data to: {output_filepath}")

    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir); print(f"Created directory: {output_dir}")
        except OSError as e:
            print(f"Error creating dir {output_dir}: {e}. Exiting."); return

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
                f.write(f"# Monitoring session started: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    except Exception as e:
        print(f"Error opening/writing header to {output_filepath}: {e}")
        return

    initial_load_seconds = getattr(config, 'STANDALONE_INITIAL_LOAD_SECONDS', 30)
    try:
        driver.get(event_url)
        print(f"Waiting for initial page load ({initial_load_seconds} seconds)...")
        time.sleep(initial_load_seconds)
        print("Initial load complete. Starting polling.")
    except TimeoutException:
        print(f"Timeout loading page {event_url}. Check URL or increase STANDALONE_INITIAL_LOAD_SECONDS in config.py.")
        return
    except Exception as e:
        print(f"Error loading page {event_url}: {e}"); return

    kick_off_datetime_obj = None
    kick_off_str = getattr(config, 'STANDALONE_KICK_OFF_DATETIME_STR', None)
    if kick_off_str:
        try:
            kick_off_datetime_obj = datetime.datetime.strptime(kick_off_str, "%Y-%m-%d %H:%M:%S")
            print(f"Kick-off datetime set to: {kick_off_datetime_obj.strftime('%Y-%m-%d %H:%M:%S')}")
        except ValueError:
            print(f"Error: Invalid STANDALONE_KICK_OFF_DATETIME_STR format '{kick_off_str}'. Expected YYYY-MM-DD HH:MM:SS. Using normal polling.")
        except Exception as e:
            print(f"Error parsing kick-off datetime: {e}. Using normal polling.")
    else:
        print("No kick-off datetime specified in config. Using normal polling interval.")
    
    loop_count = 0
    last_refresh_time = time.monotonic()  # Track last refresh
    try:
        while True:
            loop_start_time = time.monotonic()
            current_dt_obj = datetime.datetime.now()
            timestamp = current_dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] # Milliseconds

            order_book = get_order_book_data_standalone(driver)
            if order_book["yes"] or order_book["no"]:
                record = {"timestamp": timestamp, "order_book": order_book}
                try:
                    with open(output_filepath, 'a', encoding='utf-8') as f: f.write(json.dumps(record) + "\n")
                except Exception as e:
                    print(f"Error writing data to file {output_filepath}: {e}")

            # --- Refresh the page every 5 minutes (300 seconds) ---
            if time.monotonic() - last_refresh_time >= 300:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Refreshing page to keep data fresh...")
                try:
                    driver.refresh()
                    print("Page refreshed successfully.")
                except Exception as e:
                    print(f"Error refreshing page: {e}")
                last_refresh_time = time.monotonic()

            # Use getattr for polling intervals with defaults
            base_polling_interval = getattr(config, 'STANDALONE_POLLING_INTERVAL', 1.0) # Default to 1 sec
            pre_kickoff_factor = getattr(config, 'STANDALONE_POLLING_INTERVAL_PRE_KICKOFF_FACTOR', 5.0) # Default to 5x slower

            current_polling_interval = base_polling_interval
            if kick_off_datetime_obj:
                if current_dt_obj < kick_off_datetime_obj:
                    current_polling_interval = base_polling_interval * pre_kickoff_factor
                    if loop_count % 60 == 0: # Print less frequently
                         print(f"Pre-kickoff: Polling interval = {current_polling_interval:.2f}s")
                # else: # Optional: print if post-kickoff
                    # if loop_count % 60 == 0:
                    #      print(f"Post-kickoff: Polling interval = {current_polling_interval:.2f}s")
            
            loop_count += 1
            proc_time = time.monotonic() - loop_start_time
            sleep_dur = current_polling_interval - proc_time
            if sleep_dur > 0: time.sleep(sleep_dur)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")
    except Exception as e:
        print(f"Critical error in polling loop: {e}")
    finally:
        print(f"Finished monitoring single event: {event_name_for_header}")


if __name__ == "__main__":
    print("Setting up Brave web driver for single event monitoring...")
    # pdb.set_trace() # Uncomment for debugging startup if needed
    driver_instance = None # Initialize to ensure it's defined for finally block
    try:
        driver_instance = setup_standalone_driver()
        if driver_instance:
            monitor_single_event(driver_instance)
        else:
            print("Failed to initialize Brave web driver for standalone monitoring. Exiting.")
    except Exception as e:
        print(f"Error during single event monitoring: {e}")
    finally:
        if driver_instance: # Check if driver was successfully initialized
            print("Closing Brave web driver (standalone).")
            driver_instance.quit()
        else:
            print("Driver instance was not created, no need to quit.")