# MaxiZone WebSocket Client

## Overview
This client connects to the MaxiZone WebSocket endpoint and logs all received data with timestamps.

## Files Created
- `maxizone_socket.py` - Main WebSocket client

## Usage

### Basic Usage
```bash
python3 maxizone_socket.py
```

### Test Mode (without sending initial messages)
```bash
python3 maxizone_socket.py --test-without-messages
```

### Custom Game ID
```bash
python3 maxizone_socket.py --game-id 123456789
```

### With Cookies (for authentication)
```bash
python3 maxizone_socket.py --cookies "session_id=abc123; auth_token=xyz789"
```

## Configuration
- **WebSocket URL**: `wss://maxizone.win/playerzone?id=Urtrvs5XwYiXRE9bPCROPA`
- **Default Game ID**: 651719738
- **Sport**: 1 (Football)
- **Language**: "en"

## Messages Sent (when not in test mode)
1. **Protocol Message**: `{"protocol":"json","version":1}`
2. **ConnectClient Message**: 
   ```json
   {
     "arguments": [{
       "gameid": 651719738,
       "sport": 1,
       "lng": "en"
     }],
     "invocationId": "0",
     "target": "ConnectClient",
     "type": 1
   }
   ```

## Connection Status
**Current Status**: ❌ Connection Failed (409 Conflict)

### Error Details
- **HTTP Status**: Base domain (200 OK), Playerzone endpoint (400 Bad Request)
- **WebSocket Error**: 409 Conflict
- **Server**: Microsoft-IIS/10.0 (ASP.NET)
- **CORS Headers**: Correctly configured for both origins
- **Origin Header**: ✅ Fixed (now uses `https://melbet-596650.top`)

### Headers Used (from working cURL)
```
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36
Origin: https://melbet-596650.top
Cache-Control: no-cache
Accept-Language: en-GB,en-US;q=0.9,en;q=0.8
Pragma: no-cache
Sec-WebSocket-Version: 13
Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits
```

### Possible Causes
1. **Authentication Required**: WebSocket requires session cookies or auth tokens
2. **Session Management**: Need active session from melbet-596650.top
3. **SignalR Protocol**: Requires proper .NET SignalR negotiation
4. **Rate Limiting**: Server blocking automated connections

## Logging
- **Log Directory**: `logs/maxizone_data/YYYY-MM-DD/`
- **Log Format**: `[timestamp] [message_type] data`
- **Message Types**:
  - `OPEN`: Connection established
  - `IN`: Incoming messages
  - `OUT`: Outgoing messages
  - `RAW`: Non-JSON messages
  - `ERROR`: Error messages
  - `CLOSE`: Connection closed
  - `HTTP_TEST`: HTTP connection test results

## Testing Results

### Test 1: With Initial Messages
- Connection attempt with protocol and ConnectClient messages
- Result: 409 Conflict error

### Test 2: Without Initial Messages (--test-without-messages)
- Connection attempt without sending any initial messages
- Result: Same 409 Conflict error

**Conclusion**: The connection failure is not related to the initial messages. The server is rejecting the WebSocket handshake itself, likely due to authentication or session requirements.

## Recommendations

### For Working Connection
If you have access to a valid session or authentication token:
1. Add authentication headers to the WebSocket connection
2. Implement SignalR negotiation if required
3. Use browser developer tools to capture working connection headers

### Alternative Approaches
1. **Browser Automation**: Use Selenium to establish a session first
2. **Session Extraction**: Extract session cookies from a browser session
3. **API Analysis**: Analyze the web application to understand the authentication flow

## Code Features
- ✅ Timed logging with millisecond precision
- ✅ JSON message parsing and formatting
- ✅ Command-line arguments for configuration
- ✅ Error handling and connection monitoring
- ✅ Test mode for debugging
- ✅ HTTP connection testing
- ✅ Threaded WebSocket handling
- ✅ Graceful shutdown on Ctrl+C

## Dependencies
- `websocket-client==1.8.0`
- `requests`
- Standard Python libraries (json, threading, datetime, etc.)
