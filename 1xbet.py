import requests
import time
import datetime
import os
import re
import logging

def parse_curl(curl_command):
    """
    Parses a cURL command string and returns (url, headers_dict, cookies_dict).
    """
    url = ""
    headers = {}
    cookies = {}
    # Extract URL
    url_match = re.search(r"curl ['\"]([^'\"]+)['\"]", curl_command)
    if url_match:
        url = url_match.group(1)
    # Extract headers
    header_matches = re.findall(r"-H ['\"]([^'\"]+)['\"]", curl_command)
    for h in header_matches:
        if ":" in h:
            k, v = h.split(":", 1)
            headers[k.strip()] = v.strip()
    # Extract cookies
    cookie_str = headers.get("cookie") or headers.get("cookies")
    if cookie_str:
        for pair in cookie_str.split(";"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                cookies[k.strip()] = v.strip()
        headers.pop("cookie", None)
        headers.pop("cookies", None)
    return url, headers, cookies

def poll_data(url, headers, cookies):
    try:
        resp = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        data = resp.json()
        # Get the first three 'C' values from data['Value']['GE'][0]['E']
        c_values = []
        ge_list = data.get("Value", {}).get("GE", [])
        if ge_list and "E" in ge_list[0]:
            e_list = ge_list[0]["E"]
            for entry in e_list:
                # Each entry is a list of dicts, take the first dict and get 'C'
                if entry and isinstance(entry, list) and "C" in entry[0]:
                    c_values.append(entry[0]["C"])
                    if len(c_values) == 3:
                        break
        return c_values
    except Exception as e:
        logging.error(f"Error: {e}")
        return None

def print_if_changed(c_values, last_c_values, logger):
    if c_values is not None and c_values != last_c_values:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Includes milliseconds
        msg = f"[{now}] C values: {', '.join(str(c) for c in c_values)}"
        print(msg)
        logger.info(msg)
        return c_values
    return last_c_values

def main():
    # Setup logging
    log_dir = os.path.join("data", "xbet_data")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "xbet.log")
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logger = logging.getLogger("xbet_logger")

    # Prompt user for cURL command
    #curl_command = input("Paste the cURL command for the 1xBet API request:\n").strip()
    curl_command = """curl 'https://1xbetind.in/service-api/LiveFeed/GetGameZip?id=627683132&lng=en&isSubGames=true&GroupEvents=true&countevents=250&grMode=4&partner=71&topGroups=&country=71&marketType=1' \
  -H 'x-hd: eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJndWlkIjoiZjVOMHBEU0s1K25nWUZZeFNDajZyTjl3bEJoREJTYVlnV1pHTHdOYnZHUnUwVE1yTHV4blBPS3R4NGkrdkthR2NBditHQ01HOUpCUmxpckN2Z3o1NmgySkxrTFF5T092VTJHSVVFV0xFNVUvcFdMTEFYZG1OYWE0Tm9WbG1rSVdJQ2VCdC9tdWpBd3lnMVI4cWdnZkEwc3d6YW40bWc0aWRKZng4c3hzN1NtKzdKZVRDTmlRR0hVaHF6NEE1ZjRhdHFuTTY4bzZQaEtzMWdUV0hqMWxQeDV2dnRMZUtYcXlpMktnWFlIVEUrOXJhejZaM0wvR3R3c0U3WEk3ZjJsV21tYXRUbGo1SlhpakpJK1dzZi92YzBjK0hRYzdXYzFSenBsYlROajR1c3NSd01GVE9Bd1JCUEh2TktqTkE1YkVpWW1qV0x0UVhrYUpTemcrSTl5NC9yN0RZelQ0RGd5Z0J2MG5sV0E3SzhTNkhUT2dmR0hrYVE2ZTBUWG5nbmhuaitOZlpRPT0iLCJleHAiOjE3NDY5OTA2NTYsImlhdCI6MTc0Njk3NjI1Nn0.1IMKdonw_wY9CUX35QxrzBYnqzcTwl_A5CcrTWmFmAk3xEXg5b6IwZEbQ-blJO_jcSFnfjcz8WwSBST7AjHFPA' \
  -H 'x-svc-source: __BETTING_APP__' \
  -H 'sec-ch-ua-platform: "Android"' \
  -H 'is-srv: false' \
  -H 'Referer: https://1xbetind.in/en/live/football/1706694-uefa-nations-league/627683132-spain-france' \
  -H 'sec-ch-ua: "Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"' \
  -H 'sec-ch-ua-mobile: ?1' \
  -H 'x-app-n: __BETTING_APP__' \
  -H 'x-requested-with: XMLHttpRequest' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'content-type: application/json' \
  -H 'User-Agent: Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36'"""
    url, headers, cookies = parse_curl(curl_command)
    if not url:
        print("Could not parse URL from cURL command.")
        return

    print(f"Polling URL: {url}")
    last_c_values = None
    while True:
        c_values = poll_data(url, headers, cookies)
        last_c_values = print_if_changed(c_values, last_c_values, logger)
        time.sleep(0.1)

if __name__ == "__main__":
    main()