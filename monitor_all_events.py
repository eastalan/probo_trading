# monitor_all_events.py
import multiprocessing
import time
import os
import sys # For exit codes

import config # Your configuration file
from market_data_worker import record_market_data_for_event # Worker function

# It's cleaner if setup_main_driver_for_listing_initial_scan also uses a robust method
# like setup_worker_driver for its browser instance, or at least runs incognito.
# For now, assuming it's defined elsewhere (e.g., event_data.py) or we'll use a simplified one.
try:
    from event_data import setup_driver_for_event_lister as setup_main_driver_for_listing_initial_scan
    print("INFO: Successfully imported setup_main_driver_for_listing_initial_scan from event_data.py")
except ImportError:
    print("WARNING: 'event_data.py' or 'setup_driver_for_event_lister' not found.")
    print("WARNING: Initial scan for new events will be skipped if it relies on this missing function.")
    # Define a dummy function if you want the script to run without event_data.py for worker testing
    def setup_main_driver_for_listing_initial_scan():
        print("ERROR: setup_main_driver_for_listing_initial_scan is not available. Cannot perform initial scan.")
        return None

from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException


def read_events_from_psv_for_monitoring(filepath):
    events_to_monitor_stubs = []
    if not os.path.exists(filepath):
        print(f"INFO: Events file '{filepath}' not found. No events to monitor.")
        return events_to_monitor_stubs
    if os.path.getsize(filepath) == 0:
        print(f"INFO: Events file '{filepath}' is empty. No events to monitor.")
        # Consider creating it with a header if it's missing and an initial scan is expected
        try:
            with open(filepath, 'w', encoding='utf-8') as f_init_empty:
                f_init_empty.write(config.FILE_HEADER)
            print(f"INFO: Created empty events file with header: {filepath}")
        except Exception as e_create_empty:
            print(f"WARNING: Could not create empty events file {filepath}: {e_create_empty}")
        return events_to_monitor_stubs

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
            expected_header = config.FILE_HEADER.strip()
            if header_line != expected_header:
                print(f"WARNING: Events file header mismatch in '{filepath}'.")
                print(f"  Expected: '{expected_header}'")
                print(f"  Found:    '{header_line}'")
                print(f"  Will attempt to parse, but data integrity might be compromised.")
                # Depending on severity, you might want to return events_to_monitor_stubs here.

            for line_num, line in enumerate(f, 2): # Start line_num from 2 due to header
                line = line.strip()
                if not line or line.startswith("#"): continue # Skip empty lines and comments
                parts = line.split(config.DELIMITER)
                if len(parts) == config.EXPECTED_PSV_COLUMNS:
                    try:
                        # Access parts using defined column indices from config
                        trading_started_on = parts[config.PSV_COL_TRADING_STARTED_ON]
                        event_expires_on = parts[config.PSV_COL_EVENT_EXPIRES_ON]
                        event_name = parts[config.PSV_COL_EVENT_NAME]
                        event_url = parts[config.PSV_COL_URL]
                        record_status = parts[config.PSV_COL_RECORD].strip()

                        if event_name and event_url and event_url.startswith("http"):
                            if record_status == "1": # Monitor only if RecordStatus is '1'
                                events_to_monitor_stubs.append({
                                    "name": event_name,
                                    "url": event_url,
                                    "started_on": trading_started_on,
                                    "expires_on": event_expires_on
                                })
                        # else:
                        #     print(f"Debug: Skipping line {line_num} due to missing name/URL or non-http URL: {line[:50]}...")
                    except IndexError:
                        print(f"WARNING: Malformed PSV line {line_num} (IndexError, not enough parts for configured columns): {line}")
                else:
                    print(f"WARNING: Malformed PSV line {line_num}, expected {config.EXPECTED_PSV_COLUMNS} parts but got {len(parts)}: {line}")
        print(f"Read PSV '{filepath}'. Found {len(events_to_monitor_stubs)} events with Record='1' to monitor.")
    except Exception as e:
        print(f"CRITICAL: Error reading events file '{filepath}': {e}")
    return events_to_monitor_stubs


def perform_initial_scan(driver_setup_function):
    """
    Performs a one-time scan to find events and populate the PSV file.
    Returns True if new events were added or PSV was populated, False otherwise.
    """
    print("INFO: Attempting one-time scan to find and list events.")
    if not callable(driver_setup_function):
        print("ERROR: driver_setup_function for initial scan is not callable. Skipping scan.")
        return False

    temp_driver = None
    new_events_found_this_scan = False
    try:
        # The initial scan driver should also use a temp profile or incognito to avoid conflicts
        # For simplicity, we're just calling the passed function.
        # Consider passing a profile_request_info="FRESH_PROFILE_REQUEST" if its signature allows
        temp_driver = driver_setup_function() # This should ideally return a Selenium driver instance
        if not temp_driver:
            print("ERROR: Could not start WebDriver for initial scan. Scan aborted.")
            return False

        print(f"Initial Scan: Navigating to {config.STARTING_URL_EVENT_LISTER}")
        temp_driver.get(config.STARTING_URL_EVENT_LISTER)
        time.sleep(config.INITIAL_SCAN_LOAD_WAIT_SECONDS) # Wait for dynamic content

        for i in range(config.INITIAL_SCAN_SCROLL_ATTEMPTS):
            temp_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            print(f"Initial Scan: Scrolled down ({i+1}/{config.INITIAL_SCAN_SCROLL_ATTEMPTS}). Waiting...")
            time.sleep(config.SCROLL_PAUSE_TIME_EVENT_LISTER)

        # Adjust XPaths based on the actual structure of the event listing page
        event_card_links_xpath = "//a[contains(@class, 'style_home__events__link__') or contains(@class, 'event-card-link')]" # Example, make it robust
        event_elements = temp_driver.find_elements(By.XPATH, event_card_links_xpath)
        print(f"Initial Scan: Found {len(event_elements)} potential event links/cards.")

        if not event_elements:
            return False

        # Prepare to write to PSV: ensure file exists with header
        psv_file_existed_before_scan = os.path.exists(config.EVENTS_FILE_PATH) and os.path.getsize(config.EVENTS_FILE_PATH) > 0
        if not psv_file_existed_before_scan:
            with open(config.EVENTS_FILE_PATH, 'w', encoding='utf-8') as f_psv_init:
                f_psv_init.write(config.FILE_HEADER) # Assumes FILE_HEADER in config ends with \n
            print(f"Initial Scan: Created new PSV file with header: {config.EVENTS_FILE_PATH}")

        # Read existing URLs from PSV to avoid duplicates
        existing_urls_in_psv = set()
        if psv_file_existed_before_scan: # Only read if it actually existed and had content
            with open(config.EVENTS_FILE_PATH, 'r', encoding='utf-8') as f_read_existing:
                next(f_read_existing) # Skip header
                for line in f_read_existing:
                    parts = line.strip().split(config.DELIMITER)
                    if len(parts) == config.EXPECTED_PSV_COLUMNS:
                        try:
                            existing_urls_in_psv.add(parts[config.PSV_COL_URL].strip())
                        except IndexError:
                            pass # malformed line

        newly_added_count = 0
        with open(config.EVENTS_FILE_PATH, 'a', encoding='utf-8') as f_psv_append:
            for element in event_elements:
                name, url_rel, traders_text = "N/A", None, "N/A"
                try:
                    # These XPaths are examples, adjust them for the actual website structure
                    # Ensure XPaths are relative to the 'element' context
                    name_el = element.find_element(By.XPATH, ".//div[contains(@class, 'style_home__event__body__info__title__') or contains(@class, 'event-title')]")
                    name = name_el.text.strip() if name_el else "N/A"

                    url_rel = element.get_attribute('href')

                    traders_el = element.find_element(By.XPATH, ".//div[contains(@class, 'style_home__event__header__text__') or contains(@class, 'event-traders')]")
                    traders_text = traders_el.text.strip() if traders_el else "N/A"

                    if name != "N/A" and url_rel:
                        abs_url = url_rel if url_rel.startswith("http") else config.BASE_URL_PROBO + url_rel
                        if abs_url not in existing_urls_in_psv:
                            # Create a default PSV line for the new event
                            psv_parts = ["N/A"] * config.EXPECTED_PSV_COLUMNS
                            psv_parts[config.PSV_COL_EVENT_NAME] = name.replace(config.DELIMITER, " ") # Sanitize name
                            psv_parts[config.PSV_COL_URL] = abs_url
                            psv_parts[config.PSV_COL_TRADERS] = traders_text.replace(config.DELIMITER, " ") # Sanitize
                            psv_parts[config.PSV_COL_RECORD] = "0" # Default to '0' (not monitoring yet)
                            # TradingStartedOn and EventExpiresOn remain "N/A"

                            line_to_write = config.DELIMITER.join(psv_parts) + "\n"
                            f_psv_append.write(line_to_write)
                            existing_urls_in_psv.add(abs_url) # Add to set to avoid duplicates within this scan session
                            newly_added_count += 1
                            new_events_found_this_scan = True
                except NoSuchElementException:
                    # print(f"Initial Scan: Skipping an element, couldn't find all required sub-elements (name/url/traders).")
                    pass
                except Exception as e_parse_element:
                    print(f"Initial Scan: Warning - Error processing one event element: {e_parse_element}")
        if newly_added_count > 0:
            print(f"Initial Scan: Added {newly_added_count} new events to '{config.EVENTS_FILE_PATH}'.")
        else:
            print("Initial Scan: No new unique events were added to the PSV file.")

    except TimeoutException as e_timeout:
        print(f"Initial Scan: ERROR - Timeout during page navigation or element finding: {e_timeout}")
    except Exception as e_scan:
        print(f"Initial Scan: ERROR - An unexpected error occurred during the scan: {e_scan}")
    finally:
        if temp_driver:
            try:
                temp_driver.quit()
                print("Initial Scan: WebDriver quit successfully.")
            except Exception as e_quit:
                print(f"Initial Scan: Error quitting WebDriver: {e_quit}")
    return new_events_found_this_scan


if __name__ == "__main__":
    print(f"--- Main Monitoring Script Started (PID: {os.getpid()}) ---")
    print(f"Global HEADLESS_MODE (from config): {config.HEADLESS_MODE}")
    print(f"Brave Browser Path (from config): {config.BRAVE_APP_PATH}")
    if not os.path.exists(config.BRAVE_APP_PATH):
        print(f"CRITICAL ERROR: Brave Browser application not found at specified path: '{config.BRAVE_APP_PATH}'.")
        print("Please check 'BRAVE_APP_PATH' in your config.py. Exiting.")
        sys.exit(1)


    # Ensure necessary directories exist
    for dir_path in [config.EVENTS_FILE_DIR, config.MARKET_DATA_BASE_DIR]:
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
                print(f"Created directory: {dir_path}")
            except OSError as e:
                print(f"CRITICAL: Error creating directory {dir_path}: {e}. Exiting.")
                sys.exit(1)

    # Read events initially
    events_for_workers = read_events_from_psv_for_monitoring(config.EVENTS_FILE_PATH)

    # If no events are marked for monitoring and PSV file is empty/missing, try an initial scan
    psv_exists_and_has_content = os.path.exists(config.EVENTS_FILE_PATH) and os.path.getsize(config.EVENTS_FILE_PATH) > len(config.FILE_HEADER)

    if not events_for_workers and not psv_exists_and_has_content:
        print("INFO: No events currently marked for monitoring and PSV file is empty or missing header.")
        if perform_initial_scan(setup_main_driver_for_listing_initial_scan): # Pass the actual setup function
            print("INFO: Initial scan completed. Re-reading events from PSV file.")
            events_for_workers = read_events_from_psv_for_monitoring(config.EVENTS_FILE_PATH)
        else:
            print("INFO: Initial scan did not add new events or failed. Check PSV file and scan logs.")
    elif not events_for_workers and psv_exists_and_has_content:
         print(f"INFO: PSV file '{config.EVENTS_FILE_PATH}' exists but no events are currently marked with Record='1'.")
         print(f"INFO: To monitor events, edit the PSV and set the last column to '1' for desired events.")


    if not events_for_workers:
        print("FINAL: No events to monitor after all checks. Exiting.")
        sys.exit(0)

    print(f"\n--- Starting Worker Processes ---")
    print(f"Found {len(events_for_workers)} events marked with Record='1' for monitoring.")

    processes = []
    # Determine profile strategy
    use_existing_profiles_from_config = hasattr(config, 'WORKER_PROFILE_NAMES') and config.WORKER_PROFILE_NAMES
    num_configured_profiles = len(config.WORKER_PROFILE_NAMES) if use_existing_profiles_from_config else 0

    if use_existing_profiles_from_config:
        print(f"INFO: Will attempt to use {num_configured_profiles} existing Brave profiles from config: {config.WORKER_PROFILE_NAMES}")
        print(f"INFO: If an existing profile fails or is unavailable, the worker will attempt to use a fresh temporary profile.")
    else:
        print("INFO: No existing worker profiles specified in config (WORKER_PROFILE_NAMES is empty or missing).")
        print("INFO: All workers will be launched with fresh, temporary browser profiles.")

    for i, event_stub in enumerate(events_for_workers):
        profile_to_request_for_worker = "FRESH_PROFILE_REQUEST" # Default: create a new temp profile
        profile_log_display = "Fresh/Temp Profile"

        if use_existing_profiles_from_config and num_configured_profiles > 0:
            # Cycle through the list of configured existing profiles
            specific_profile_name = config.WORKER_PROFILE_NAMES[i % num_configured_profiles]
            profile_to_request_for_worker = specific_profile_name
            profile_log_display = f"Existing Profile '{specific_profile_name}'"

        process_args = (
            event_stub["name"],
            event_stub["url"],
            event_stub["started_on"],
            event_stub["expires_on"],
            profile_to_request_for_worker # This tells the worker what kind of profile to use
        )

        process = multiprocessing.Process(target=record_market_data_for_event, args=process_args)
        processes.append(process)
        try:
            process.start()
            print(f"  Launched worker (PID: {process.pid}) for: \"{event_stub['name'][:50]}...\" with {profile_log_display}")
        except Exception as e_proc_start:
            print(f"ERROR: Failed to start process for \"{event_stub['name']}\": {e_proc_start}")
            # Optionally remove from 'processes' list if it failed to start, or handle later
            continue # Try next event

        # Stagger process launches to avoid overwhelming resources, especially with browser startups
        time.sleep(config.WORKER_LAUNCH_DELAY_SECONDS)

    if not processes:
        print("FINAL: No worker processes were successfully launched. Exiting.")
        sys.exit(1)

    print(f"\n--- All {len(processes)} worker processes launched. Monitoring active. ---")
    print("Press Ctrl+C in this terminal to stop all workers and exit.")

    try:
        for process in processes:
            process.join() # Wait for all processes to complete
    except KeyboardInterrupt:
        print("\n--- KeyboardInterrupt received. Terminating worker processes gracefully... ---")
        for idx, p in enumerate(processes):
            print(f"Terminating worker {idx+1}/{len(processes)} (PID: {p.pid})...")
            if p.is_alive():
                try:
                    p.terminate() # Send SIGTERM
                    p.join(timeout=10) # Wait for up to 10 seconds for graceful shutdown
                    if p.is_alive():
                        print(f"  Worker PID {p.pid} did not terminate gracefully after 10s. Sending SIGKILL...")
                        p.kill()    # Send SIGKILL
                        p.join(timeout=5) # Wait for kill
                        if p.is_alive():
                             print(f"  ERROR: Worker PID {p.pid} could not be killed.")
                        else:
                             print(f"  Worker PID {p.pid} killed.")
                    else:
                        print(f"  Worker PID {p.pid} terminated gracefully.")
                except Exception as e_term:
                    print(f"  Error during termination of PID {p.pid}: {e_term}")
            else:
                print(f"  Worker PID {p.pid} was already terminated.")
        print("--- All workers instructed to terminate. ---")
    except Exception as e_main_manager:
        print(f"--- Main manager encountered an unexpected error: {e_main_manager} ---")
    finally:
        print("\n--- Main monitoring script cleaning up and finishing. ---")
        # Final check for any lingering alive processes, though terminate/kill should handle most.
        # This is a last resort.
        # for p in processes:
        #     if p.is_alive():
        #         print(f"WARNING: Process PID {p.pid} still alive during final cleanup. Attempting kill.")
        #         p.kill()
        #         p.join(1)
        print("--- Script finished. ---")
        sys.exit(0)