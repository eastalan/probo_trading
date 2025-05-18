# config.py
import os

# --- General Configurations ---
EVENTS_FILE_DIR = os.path.join("data", "event_data")
EVENTS_FILE_NAME = "probo_events_relational.psv"
EVENTS_FILE_PATH = os.path.join(EVENTS_FILE_DIR, EVENTS_FILE_NAME)

MARKET_DATA_BASE_DIR = os.path.join("data", "market_data")
#SINGLE_EVENT_OUTPUT_DIR = os.path.join("data", "single_event_market_data")

# --- Brave Browser Configuration (macOSWORKER_LAUNCH_DELAY_SECONDS = 2  # or 1, or whatever delay you want between launches) ---
BRAVE_APP_PATH = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"

CHECK_FOR_LOGIN_OVERLAY = True
XPATH_LOGIN_SIGNUP_OVERLAY = "//*[@id='login_btn_navbar']"
WAIT_FOR_LOGIN_OVERLAY_SECONDS = 15
# Profile for event_data.py (main script for listing events)
BRAVE_PROFILE_TO_USE_EVENT_LISTER = "Default"

# Profile for probo_monitor.py (standalone single event monitor)
BRAVE_PROFILE_TO_USE_STANDALONE = "Default"

# MODIFICATION: List of Brave profile FOLDER NAMES for workers
# Find these folder names in: ~/Library/Application Support/BraveSoftware/Brave-Browser/
# Examples: "Profile 1", "Profile 2", "Profile 3", etc.
# Ensure each profile in this list has been manually logged into Probo beforehand.
WORKER_PROFILE_NAMES = ["Default","Profile 2", "worker2", "worker3"] # !!! UPDATE THESE TO YOUR ACTUAL PROFILE FOLDER NAMES !!!
WORKER_PROFILE_NAMES = []

# --- Global Headless Mode Configuration ---
HEADLESS_MODE = True # CHANGE THIS TO True FOR ALL SCRIPTS TO RUN HEADLESS

# --- Worker Process Configuration ---
INITIAL_PAGE_LOAD_WAIT_SECONDS_WORKER = 15
WORKER_POLLING_INTERVAL_SECONDS = 1.0 / 5.0
WORKER_LAUNCH_DELAY_SECONDS = 20  # or 1, or whatever delay you want between launches

# --- XPaths for Order Book Data ---
XPATH_YES_ALL_PRICES = "//table[.//th//font[contains(text(),'Yes') and @color='#197BFF']]//td[contains(@class, 'style_order__book__table__left__')]"
XPATH_YES_ALL_QTYS = "//table[.//th//font[contains(text(),'Yes') and @color='#197BFF']]//td[contains(@class, 'style_order__book__table__right__')]"
XPATH_NO_ALL_PRICES = "//table[.//th//font[contains(text(),'No') and @color='#DC2804']]//td[contains(@class, 'style_order__book__table__left__')]"
XPATH_NO_ALL_QTYS = "//table[.//th//font[contains(text(),'No') and @color='#DC2804']]//td[contains(@class, 'style_order__book__table__right__')]"

# --- PSV File Configuration ---
FILE_HEADER = "Trading Started On|Event Expires On|Event Name|Trader Count|URL|Record\n"
EXPECTED_PSV_COLUMNS = 6
DELIMITER = "|"
PSV_COL_TRADING_STARTED_ON = 0
PSV_COL_EVENT_EXPIRES_ON = 1
PSV_COL_EVENT_NAME = 2
PSV_COL_TRADER_COUNT = 3
PSV_COL_URL = 4
PSV_COL_RECORD = 5

# --- Event Lister Script (event_data.py) Configuration ---
STARTING_URL_EVENT_LISTER = "https://probo.in/events/football"
SCROLL_ATTEMPTS_EVENT_LISTER = 10
SCROLL_PAUSE_TIME_EVENT_LISTER = 3
PAGE_LOAD_TIMEOUT_EVENT_LISTER = 25
ELEMENT_VISIBILITY_TIMEOUT_EVENT_LISTER = 15
BASE_URL_PROBO = "https://probo.in"

# --- Standalone Monitor (probo_monitor.py) Configuration ---
STANDALONE_EVENT_URL_TO_MONITOR = "https://probo.in/events/everton-to-win-against-southampton-paeyjn?categoryI=4"
STANDALONE_INITIAL_LOAD_SECONDS = 3
STANDALONE_POLLING_INTERVAL = 1.0 / 5.0

STANDALONE_KICK_OFF_DATETIME_STR = "2025-05-17 00:00:00"
STANDALONE_POLLING_INTERVAL_PRE_KICKOFF_FACTOR = 20
