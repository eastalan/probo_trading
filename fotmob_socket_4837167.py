import websocket
import threading
import json
import time
import os
import datetime
import pandas as pd
from log_utils import get_dated_log_path

# Configuration
EVENT_UUID = "kxz64yu7ddap0d1agwdxugwk"#"10jpwd1x2paywtbfatjev182c"  # Example event UUID
MATCH_ID = "4837167"  # Example match ID for log filename
OUTLET_ID = "1hugdxmjczoe21vm8x6kz0zx82"
TOKEN = "85372212-5e58-4988-9df1-668199ec69e5"

# Connection state
OUTLET_SENT = False
CSB_INIT_SENT = False
SUBSCRIBED = False

# Match monitoring state
last_message_time = time.time()
highest_minute = 0
match_ended = False

# Prepare log directory and file - use MATCH_ID for filename
log_path = get_dated_log_path("fotmob_data", f"{MATCH_ID}.log")

def log_data(message_type, payload):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    collapsed_json = json.dumps(payload, separators=(",", ":"))
    log_line = f"[{now}] [{message_type}] {collapsed_json}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_line)
    print(f"[{now}] [{message_type}] {collapsed_json}")

def on_message(ws, message):
    global OUTLET_SENT, CSB_INIT_SENT, SUBSCRIBED
    
    try:
        data = json.loads(message)
        
        # Handle welcome message - send outlet info
        if "welcome" in data and not OUTLET_SENT:
            print("Welcome received, sending outlet info...")
            outlet_msg = {
                "outlet": {
                    "OutletKeyService": {
                        "outletid": OUTLET_ID
                    }
                }
            }
            ws.send(json.dumps(outlet_msg))
            OUTLET_SENT = True
            log_data("OUT", outlet_msg)
            
        # Handle outlet authorization - send CSB init
        elif "outlet" in data and data["outlet"].get("msg") == "is_authorised" and not CSB_INIT_SENT:
            print("Outlet authorized, sending CSB init...")
            time.sleep(0.5)
            csb_init_msg = {
                "csb": {
                    "name": "init",
                    "token": TOKEN,
                    "apptype": "OptaWidgetsV3",
                    "version": "3.248.1",
                    "referer": "pub.fotmob.com",
                    "topReferer": "www.fotmob.com"
                }
            }
            ws.send(json.dumps(csb_init_msg))
            CSB_INIT_SENT = True
            log_data("OUT", csb_init_msg)
            
        # Handle CSB init response - subscribe to event
        elif "csb" in data and data["csb"].get("msg") == "init ok" and not SUBSCRIBED:
            print("CSB init successful, subscribing to event...")
            time.sleep(0.5)
            subscribe_msg = {
                "csb": {
                    "name": "subscribe",
                    "messagetype": ["POEM"],
                    "sendLastData": True,
                    "eventuuid": EVENT_UUID
                }
            }
            ws.send(json.dumps(subscribe_msg))
            SUBSCRIBED = True
            log_data("OUT", subscribe_msg)
            
        # Handle subscription confirmation
        elif "csb" in data and data["csb"].get("msg") == "Client subscribed":
            print(f"Successfully subscribed to event {EVENT_UUID}")
            log_data("IN", data)
            
        # Handle POEM data (match events)
        elif "csb" in data and "POEM" in data["csb"]:
            global last_message_time, highest_minute, match_ended
            print(f"\n>> POEM Event Received")
            poem_data = data["csb"]["POEM"]
            log_data("POEM", poem_data)
            
            # Update last message time
            last_message_time = time.time()
            
            # Extract minute from POEM data
            if "POEM" in poem_data and "M" in poem_data["POEM"]:
                current_minute = poem_data["POEM"]["M"]
                if current_minute > highest_minute:
                    highest_minute = current_minute
                    print(f"Match minute: {current_minute}")
            
        # Handle any other messages
        else:
            print(f"Other message: {json.dumps(data, indent=2)}")
            log_data("IN", data)
            
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        print(f"Raw message: {message}")
    except Exception as e:
        print(f"Error processing message: {e}")
        print(f"Raw message: {message}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} - {close_msg}")

def on_open(ws):
    print("FotMob WebSocket connection opened.")

# WebSocket URL and headers from cURL
ws_url = "wss://csb.performgroup.io/?topreferer=www.fotmob.com"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": "https://pub.fotmob.com",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache"
}

# Create WebSocket connection
ws = websocket.WebSocketApp(
    ws_url,
    header=[f"{k}: {v}" for k, v in headers.items()],
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

def mark_match_ended():
    """Mark the match as ended in the PSV file"""
    try:
        psv_path = os.path.join("data", "event_data", "fotmob_matches.psv")
        if os.path.exists(psv_path):
            df = pd.read_csv(psv_path, delimiter="|", dtype=str)
            df.loc[df['MatchID'] == MATCH_ID, 'HasEnded'] = '1'
            df.to_csv(psv_path, sep="|", index=False)
            print(f"Marked match {MATCH_ID} as ended in PSV file")
    except Exception as e:
        print(f"Error marking match as ended: {e}")

def monitor_match_end():
    """Monitor for match end conditions"""
    global match_ended
    while not match_ended:
        time.sleep(30)  # Check every 30 seconds
        
        # Check if match should end (M>=90 and no messages for 3+ minutes)
        if highest_minute >= 90:
            time_since_last_message = time.time() - last_message_time
            if time_since_last_message >= 180:  # 3 minutes
                print(f"Match end detected: M={highest_minute}, {time_since_last_message:.1f}s since last message")
                mark_match_ended()
                match_ended = True
                ws.close()
                break

# Start WebSocket in a separate thread
print(f"Starting FotMob WebSocket client for event: {EVENT_UUID}")
ws_thread = threading.Thread(target=ws.run_forever)
ws_thread.start()

# Start match monitoring thread
monitor_thread = threading.Thread(target=monitor_match_end)
monitor_thread.start()

# Wait for threads to complete
ws_thread.join()
monitor_thread.join()
