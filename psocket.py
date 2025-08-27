import websocket
import threading
import json
import time
import os
import datetime
from log_utils import get_dated_log_path

EVENT_ID = "4383763"#"42["subscribe_ltp_stream",4383757]81]
SUBSCRIBE_SENT = False

# Prepare log directory and file
log_path = get_dated_log_path("socketdata", f"{EVENT_ID}.log")

def log_orderbook(payload):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    collapsed_json = json.dumps(payload, separators=(",", ":"))
    log_line = f"[{now}] {collapsed_json}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_line)
    print(f"[{now}] {collapsed_json}")

def on_message(ws, message):
    global SUBSCRIBE_SENT

    if message.startswith("0"):
        print("Handshake received")
        ws.send("40")

    elif message.startswith("40") and not SUBSCRIBE_SENT:
        print("Socket.IO connection acknowledged")
        time.sleep(1)
        subscribe_msg = ["subscribe_orderbook", EVENT_ID]
        ws.send("42" + json.dumps(subscribe_msg))
        SUBSCRIBE_SENT = True
        print(f"Subscribed to order book for event {EVENT_ID}")

    elif message == "2":
        ws.send("3")  # Pong
        print("[Pong sent]")

    elif message.startswith("42"):
        try:
            data = json.loads(message[2:])
            event_name, payload = data
            if "orderbook" in event_name:
                print(f"\n>> {event_name}")
                #print(json.dumps(payload, indent=2))
                log_orderbook(payload)
        except Exception as e:
            print("Parse error:", e)

    else:
        print("RAW:", message)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, code, reason):
    print("Closed:", code, reason)

def on_open(ws):
    print("WebSocket connection opened.")

ws_url = "wss://falcon.api.probo.in/socket.io/?EIO=4&transport=websocket"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Origin": "https://probo.in"
}
ws = websocket.WebSocketApp(
    ws_url,
    header=[f"{k}: {v}" for k, v in headers.items()],
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)
threading.Thread(target=ws.run_forever).start() # type: ignore