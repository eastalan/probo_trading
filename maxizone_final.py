import websocket
import threading
import json
import time
import datetime
import ssl
import argparse
import requests
from utils.core.log_utils import get_dated_log_path

# Configuration
GAME_ID = 651744976
SPORT = 1
LANGUAGE = "en"

# Connection state
PROTOCOL_SENT = False
CONNECT_CLIENT_SENT = False
SEND_NO_MESSAGES = False
SESSION_ID = None
MAX_RECONNECT_ATTEMPTS = 3
RECONNECT_DELAY = 10

# Log path will be set after parsing arguments
log_path = None

def get_signalr_token():
    """Get fresh SignalR connection token from negotiate endpoint"""
    negotiate_url = "https://maxizone.win/playerzone/negotiate?negotiateVersion=1"
    
    headers = {
        "accept": "*/*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cache-control": "max-age=0",
        "content-length": "0",
        "content-type": "text/plain;charset=UTF-8",
        "origin": "https://melbet-596650.top",
        "priority": "u=1, i",
        "referer": "https://melbet-596650.top/",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "sec-fetch-storage-access": "active",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
        "x-signalr-user-agent": "Microsoft SignalR/6.0 (6.0.25; Unknown OS; Browser; Unknown Runtime Version)"
    }
    
    try:
        response = requests.post(negotiate_url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        connection_token = data.get("connectionToken")
        print(f"SignalR negotiate success - Token: {connection_token}")
        
        return connection_token
        
    except requests.exceptions.RequestException as e:
        print(f"SignalR negotiate failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Failed to parse negotiate response: {e}")
        return None

def log_data(message_type, payload):
    """Log data with timestamp and message type"""
    # Skip logging heartbeat messages (type 6)
    if isinstance(payload, dict) and payload.get("type") == 6:
        return
    if isinstance(payload, str):
        if '"type":6' in payload or payload == '{"type":6}':
            return
        # Check for heartbeat response strings
        if "Heartbeat response:" in payload:
            return
        
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(payload, dict) or isinstance(payload, list):
        collapsed_json = json.dumps(payload, separators=(",", ":"))
    else:
        collapsed_json = str(payload)
    log_line = f"[{now}] [{message_type}] {collapsed_json}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_line)

def on_message(ws, message):
    """Handle incoming WebSocket messages"""
    global CONNECT_CLIENT_SENT, GAME_ID, SPORT, LANGUAGE
    
    # Skip logging heartbeat messages before calling log_data
    try:
        clean_message = message.rstrip("\x1e")
        if clean_message:
            data = json.loads(clean_message)
            if isinstance(data, dict) and data.get("type") == 6:
                # Handle heartbeat silently - don't log
                heartbeat_response = {"type": 6}
                heartbeat_text = json.dumps(heartbeat_response) + "\x1e"
                ws.send(heartbeat_text)
                return
    except json.JSONDecodeError:
        pass
    
    log_data("IN", message)
    
    # Handle SignalR handshake acknowledgment (empty JSON object)
    if message.strip() == "{}":
        print("Handshake acknowledged - sending ConnectClient")
        time.sleep(0.1)
        
        # Send ConnectClient as SignalR invocation
        connect_client_msg = {
            "arguments": [{"gameid": GAME_ID, "sport": SPORT, "lng": LANGUAGE}],
            "invocationId": "0",
            "target": "ConnectClient",
            "type": 1
        }
        connect_text = json.dumps(connect_client_msg) + "\x1e"
        ws.send(connect_text)
        CONNECT_CLIENT_SENT = True
        log_data("OUT", connect_client_msg)
        print("ConnectClient message sent")
        return
    
    # Try to parse and display structured data
    try:
        clean_message = message.rstrip("\x1e")
        if clean_message:
            data = json.loads(clean_message)
            if isinstance(data, dict):
                # Only print game data messages
                if data.get("target") == "gameData":
                    print(f"Game data: {json.dumps(data)}")
    except json.JSONDecodeError:
        pass

def on_error(ws, error):
    """Handle WebSocket errors"""
    print(f"WebSocket error: {error}")
    log_data("ERROR", f"WebSocket error: {error}")
    
    # Check if it's a connection reset error - close the connection to trigger reconnection
    if "Connection reset by peer" in str(error) or "[Errno 54]" in str(error):
        print("Connection reset detected - closing connection to trigger reconnection")
        log_data("ERROR", "Connection reset by peer detected - closing connection")
        ws.close()

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket close"""
    print(f"Connection closed: {close_status_code} - {close_msg}")
    log_data("CLOSE", f"Connection closed: {close_status_code} - {close_msg}")
    
    # Reset connection state flags for reconnection
    global PROTOCOL_SENT, CONNECT_CLIENT_SENT
    PROTOCOL_SENT = False
    CONNECT_CLIENT_SENT = False
    
    # Mark that reconnection should happen
    ws.should_reconnect = True

def on_open(ws):
    """Called when WebSocket connection is opened"""
    print("Connected to MaxiZone")
    log_data("OPEN", "Connection established")
    
    if not SEND_NO_MESSAGES:
        # Send SignalR handshake request first
        handshake_msg = {"protocol": "json", "version": 1}
        handshake_text = json.dumps(handshake_msg) + "\x1e"
        ws.send(handshake_text)
        global PROTOCOL_SENT
        PROTOCOL_SENT = True
        log_data("OUT", handshake_msg)
        print("SignalR handshake sent")
    else:
        print("No messages mode - waiting for server data")

def create_websocket():
    """Create WebSocket connection"""
    # Get fresh SignalR token if no session ID provided
    session_id = SESSION_ID
    if not session_id:
        print("Getting fresh SignalR connection token...")
        session_id = get_signalr_token()
        if not session_id:
            print("Failed to get SignalR token, connecting without session ID")
    
    # Use session ID if available, otherwise connect without it
    if session_id:
        ws_url = f"wss://maxizone.win/playerzone?id={session_id}"
        print(f"Connecting with session ID: {session_id}")
    else:
        ws_url = "wss://maxizone.win/playerzone"
        print("Connecting without session ID")
    
    # Headers matching the curl command (excluding WebSocket-specific headers that are auto-generated)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Origin": "https://melbet-596650.top",
        "Cache-Control": "no-cache",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Pragma": "no-cache"
    }
    
    return websocket.WebSocketApp(
        ws_url,
        header=[f"{k}: {v}" for k, v in headers.items()],
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

def connect_with_retry():
    """Connect to WebSocket with retry logic"""
    attempt = 0
    
    while attempt <= MAX_RECONNECT_ATTEMPTS:
        try:
            if attempt == 0:
                print("Initial connection attempt")
                log_data("INFO", "Initial connection attempt")
            else:
                print(f"Reconnection attempt {attempt}/{MAX_RECONNECT_ATTEMPTS}")
                log_data("INFO", f"Reconnection attempt {attempt}/{MAX_RECONNECT_ATTEMPTS}")
            
            ws = create_websocket()
            ws.should_reconnect = False
            ws.run_forever()
            
            # Check if we should reconnect after connection closes
            if hasattr(ws, 'should_reconnect') and ws.should_reconnect and attempt < MAX_RECONNECT_ATTEMPTS:
                attempt += 1
                print(f"Connection lost - waiting {RECONNECT_DELAY} seconds before reconnection...")
                log_data("INFO", f"Connection lost - waiting {RECONNECT_DELAY} seconds before reconnection attempt {attempt}")
                time.sleep(RECONNECT_DELAY)
                continue
            else:
                # Normal exit or max attempts reached
                if attempt >= MAX_RECONNECT_ATTEMPTS:
                    print(f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached")
                    log_data("ERROR", f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached")
                break
            
        except KeyboardInterrupt:
            print("\nStopped by user")
            log_data("INFO", "Stopped by user")
            break
        except Exception as e:
            error_str = str(e)
            print(f"Connection failed: {error_str}")
            log_data("ERROR", f"Connection attempt failed: {error_str}")
            
            # Check if it's a recoverable connection error
            if ("Connection reset by peer" in error_str or 
                "[Errno 54]" in error_str or
                "Connection refused" in error_str or
                "Connection aborted" in error_str):
                
                if attempt < MAX_RECONNECT_ATTEMPTS:
                    attempt += 1
                    print(f"Waiting {RECONNECT_DELAY} seconds before retry...")
                    log_data("INFO", f"Waiting {RECONNECT_DELAY} seconds before retry {attempt}")
                    time.sleep(RECONNECT_DELAY)
                else:
                    print(f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached")
                    log_data("ERROR", f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached")
                    break
            else:
                # For other errors, don't retry
                print(f"Non-recoverable error: {error_str}")
                log_data("ERROR", f"Non-recoverable error: {error_str}")
                break

def main():
    """Main function"""
    global GAME_ID, SPORT, LANGUAGE, SEND_NO_MESSAGES, SESSION_ID, log_path
    
    parser = argparse.ArgumentParser(description="MaxiZone WebSocket Client - Final Version")
    parser.add_argument("--game-id", type=int, default=652230787, help="Game ID to connect to")
    parser.add_argument("--sport", type=int, default=1, help="Sport type (default: 1)")
    parser.add_argument("--language", type=str, default="en", help="Language code (default: en)")
    parser.add_argument("--session-id", type=str, help="Session ID for connection (optional)")
    parser.add_argument("--no-messages", action="store_true", help="Don't send any messages, just listen")
    
    args = parser.parse_args()
    
    global GAME_ID, SPORT, LANGUAGE, SESSION_ID, SEND_NO_MESSAGES
    GAME_ID = args.game_id
    SPORT = args.sport
    LANGUAGE = args.language
    SESSION_ID = args.session_id
    SEND_NO_MESSAGES = args.no_messages
    
    # Set log path based on the actual game ID from arguments
    log_path = get_dated_log_path("maxizone_data", f"final_{GAME_ID}.log")
    
    print("MaxiZone WebSocket Client")
    print(f"Game ID: {GAME_ID}")
    print(f"Sport: {SPORT}")
    print(f"Language: {LANGUAGE}")
    print(f"Session ID: {SESSION_ID}")
    print(f"No messages mode: {SEND_NO_MESSAGES}")
    print(f"Log file: {log_path}")
    print(f"Max reconnect attempts: {MAX_RECONNECT_ATTEMPTS}")
    print(f"Reconnect delay: {RECONNECT_DELAY} seconds")
    print("=" * 50)
    print("Running... Press Ctrl+C to stop")
    
    try:
        connect_with_retry()
    except KeyboardInterrupt:
        print("\nStopped by user")
        log_data("INFO", "Stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        log_data("ERROR", f"Fatal error: {e}")
    
    print("\nMaxiZone client shutting down")
    log_data("INFO", "MaxiZone client shutting down")

if __name__ == "__main__":
    main()
