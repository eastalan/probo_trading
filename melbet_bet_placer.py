import requests
import json
import time
import datetime
import logging
from utils.core.log_utils import get_dated_log_path

class MelbetBetPlacer:
    def __init__(self):
        self.base_url = "https://melbet-596650.top"
        self.betting_endpoint = "/service-api/LiveBet/Secure/MakeBetWeb"
        self.update_coupon_endpoint = "/service-api/LiveBet-update/Open/UpdateCoupon"
        
        # Setup logging
        log_path = get_dated_log_path("melbet_betting", "bet_placer.log")
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )
        self.logger = logging.getLogger("melbet_bet_placer")
        print(f"Logging to: {log_path}")
        
        # Authentication headers and cookies from your session
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'content-type': 'application/json',
            'is-srv': 'false',
            'origin': 'https://melbet-596650.top',
            'priority': 'u=1, i',
            'referer': 'https://melbet-596650.top/en/live/football',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
            'x-app-n': '__BETTING_APP__',
            'x-auth': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjEiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiI1MC8xMzcwMDIyNDczIiwicGlkIjoiOCIsImp0aSI6IjAvYmJjMTdhYTljMzcwNWNkOTgxZGFlZjI5OTRiZTU2YmFhZmIzYzU1ZDc5ZDQwNTcwMjY4MzZiOGU2ZDQ3OWEwOCIsImFwcCI6Ik5BIiwiaW5uZXIiOiJ0cnVlIiwibmJmIjoxNzU2NTM5NDM0LCJleHAiOjE3NTY1NTM4MzQsImlhdCI6MTc1NjUzOTQzNH0.AoyQnW3VUV2GEfv_16ga7nvHf6nJbiBgK_ILV-rzxx7FVY6hFUWJazdKk5_lcA0iZiW-yPrPxPNM8AzoqvvQ8A',
            'x-hd': 'eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZXYiOjEsImd1aWQiOiJXR0NRaUYxVVhtSzN6WFVJT0Nka1BRZ0RDRG5zN3pnaHBVU1diTTNNL3dpSERXMzF3Uy9DbEtMWDE5SGVnaVZvNEVpbkN6cmpzL0tHKzMzMWtkZE9nYjkyK1FQeFZaaUZ1TDZnUllsWUJWTUdqVVRtVG1peU02UXpxWXJSZUVTQkphU3g1YWVZQUFjbnRoR1JSZmhOQ2dNdHlHOUVtSHlRMFFqNlgrWXVSemd4ZXJLVUZNbnNwZXdGOWJjVmxWb0RxMkdzWEVRNVJUaWR3N2NiTlIzTFVQUnc5MjM4ZHRhbWFJSE4zRmlDV0JTc0xaRHA4dWJhbktSVzNIRHY3cTZzRTdac0cxME9VYk4wT3V1YURSWXA4U3cya2tCaitxektueEplMVdUTEhMUmxaM3FHblNBaWQzSjdYL0h6N3hyTnFSOE9RYlQvMC9MMDRWYzJGMW93S1BnRzR2dXZaWi9TVnVpeEMwSkpJc1prQjU5Mm84T2VYU09zN1lQUlB4eVM5cGpBbWIwenQ5OVdsLzNSZFNYbHRNR3NJa1lEUzVRNk8rMW44NkhpWGhETEZKUUJDb1F4cDZnNld4NDZNclhyNTRWRDR0R0s1MzFPTjZxQmRDREdYUkUreDhmUHMzMEcyTlBMM2g3cVJPemtlbWRCLzFJQ0J4THFIc2V5c0VXRnlOQzhzSzlIdVpZT2M5KzRESTM2Q2xxZkNPZmxHektSc3FLeDFMVWNJL1B0eW9GekZpTTU4b2JwUHhIRG15WVpEZFhUVDh2aXZEc2l0d21mS1ozRWd0alNnRVcvR3B3SHVUQXlrUWRaIiwiZXhwIjoxNzU2NTUzNzkzLCJpYXQiOjE3NTY1MzkzOTN9.szrl8t_MrdV7twIufyhm6QxkyZ791dohQCpG6SFIMH4YWHvjUJYlWTFPqzz8DyU3Vo3Ryb0RoXK-5VauoFaNPQ',
            'x-requested-with': 'XMLHttpRequest',
            'x-svc-source': '__BETTING_APP__'
        }
        
        self.cookies = {
            'platform_type': 'desktop',
            'gw-blk': 'eyJkYXRhIjp7ImlkIjowLCJkaXNwbGF5VHlwZUlkIjowLCJ0ZW1wbGF0ZVR5cGVJZCI6MCwidGVtcGxhdGVJZCI6MH0sImJyZWFkY3J1bWJzIjpudWxsfQ==',
            'lng': 'en',
            'cookies_agree_type': '3',
            'tzo': '5.5',
            'is12h': '0',
            'auid': 'Z29zZGivaf999nlCCVd6Ag==',
            'che_g': '1a93e6c4-8bfe-432b-91e2-5d526bd73450',
            'sh.session.id': '6af617fc-90be-4334-a3f3-be8695273c79',
            'application_locale': 'en',
            'x-banner-api': '',
            '_gcl_au': '1.1.127769253.1756326412',
            'sbjs_migrations': '1418474375998%3D1',
            'sbjs_current_add': 'fd%3D2025-08-28%2001%3A56%3A51%7C%7C%7Cep%3Dhttps%3A%2F%2Fmelbet-596650.top%2Fen%2Flive%2Ffootball%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F',
            'sbjs_first_add': 'fd%3D2025-08-28%2001%3A56%3A51%7C%7C%7Cep%3Dhttps%3A%2F%2Fmelbet-596650.top%2Fen%2Flive%2Ffootball%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F',
            'sbjs_current': 'typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29',
            'sbjs_first': 'typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29',
            '_gid': 'GA1.2.1412766403.1756502650',
            '_glhf': '1756557167',
            'ggru': '160',
            'ua': '1370022473',
            'uhash': '610d4b4b2df94a50aabb037a641d4c88',
            'cur': 'INR',
            'user_token': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjEiLCJ0eXAiOiJKV1QifQ.eyJzdWIiOiI1MC8xMzcwMDIyNDczIiwicGlkIjoiOCIsImp0aSI6IjAvYmJjMTdhYTljMzcwNWNkOTgxZGFlZjI5OTRiZTU2YmFhZmIzYzU1ZDc5ZDQwNTcwMjY4MzZiOGU2ZDQ3OWEwOCIsImFwcCI6Ik5BIiwiaW5uZXIiOiJ0cnVlIiwibmJmIjoxNzU2NTM5NDM0LCJleHAiOjE3NTY1NTM4MzQsImlhdCI6MTc1NjUzOTQzNH0.AoyQnW3VUV2GEfv_16ga7nvHf6nJbiBgK_ILV-rzxx7FVY6hFUWJazdKk5_lcA0iZiW-yPrPxPNM8AzoqvvQ8A',
            'newuser_review': '928914561',
            'reg_id': 'ad25e049bef49265c4b8b6e396bc2279',
            'firstAuthRedirect': '1',
            'SESSION': '97883c1b7a1dd8a40fed7b3de7a37ae8',
            'post_reg_type': 'phone_reg',
            'PAY_SESSION': '20e5c44e447d0d0b153de9a6f45bb9b6',
            'sbjs_udata': 'vst%3D4%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Linux%3B%20Android%206.0%3B%20Nexus%205%20Build%2FMRA58N%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F139.0.0.0%20Mobile%20Safari%2F537.36',
            'sbjs_session': 'pgs%3D3%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fmelbet-596650.top%2Fen%2Flive%2Ffootball',
            '_ga': 'GA1.2.98088144.1756326412',
            '_gat_UA-244626893-1': '1',
            'window_width': '842',
            '_ga_435XWQE678': 'GS2.1.s1756539376$o4$g1$t1756540355$j29$l0$h0',
            '_ga_8SZ536WC7F': 'GS2.1.s1756539376$o4$g1$t1756540355$j19$l1$h73569795'
        }

    def update_coupon(self, bet_data):
        """Update coupon before placing bet"""
        url = f"{self.base_url}{self.update_coupon_endpoint}"
        
        # Create update coupon payload
        update_payload = {
            "UserId": bet_data["UserId"],
            "Events": bet_data["Events"],
            "Vid": bet_data.get("Vid", 0),
            "partner": bet_data.get("partner", 8),
            "Lng": bet_data.get("Lng", "en"),
            "CfView": 0,
            "CalcSystemsMin": False,
            "Group": bet_data.get("Group", 1182),
            "Country": 71,
            "Currency": 99,
            "SaleBetId": 0,
            "IsPowerBet": bet_data.get("IsPowerBet", False),
            "WithLobby": False,
            "IsExpressBoost": True
        }
        
        try:
            response = requests.post(
                url,
                headers=self.headers,
                cookies=self.cookies,
                json=update_payload,
                timeout=10
            )
            
            self.logger.info(f"Update coupon request: {json.dumps(update_payload, indent=2)}")
            self.logger.info(f"Update coupon response: {response.status_code} - {response.text}")
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Update coupon failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error updating coupon: {str(e)}")
            return None

    def place_bet(self, bet_data):
        """Place a bet using the Melbet API"""
        url = f"{self.base_url}{self.betting_endpoint}"
        
        try:
            # First update the coupon
            print("Updating coupon...")
            coupon_result = self.update_coupon(bet_data)
            if not coupon_result:
                print("Failed to update coupon")
                return None
            
            # Now place the bet
            print("Placing bet...")
            response = requests.post(
                url,
                headers=self.headers,
                cookies=self.cookies,
                json=bet_data,
                timeout=10
            )
            
            # Log the request and response
            self.logger.info(f"Bet request: {json.dumps(bet_data, indent=2)}")
            self.logger.info(f"Bet response: {response.status_code} - {response.text}")
            
            print(f"Response status: {response.status_code}")
            print(f"Response: {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get("Success"):
                    print("✅ Bet placed successfully!")
                    print(f"Bet ID: {result.get('Value', {}).get('Id')}")
                    print(f"Balance: {result.get('Value', {}).get('Balance')}")
                    return result
                else:
                    print(f"❌ Bet failed: {result.get('Error', 'Unknown error')}")
                    return result
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                return None
                
        except Exception as e:
            error_msg = f"Error placing bet: {str(e)}"
            print(f"❌ {error_msg}")
            self.logger.error(error_msg)
            return None

    def create_bet_payload(self, user_id, game_id, bet_type, coef, amount, **kwargs):
        """Create a bet payload with the required structure"""
        payload = {
            "UserId": user_id,
            "Events": [
                {
                    "GameId": game_id,
                    "Type": bet_type,
                    "Coef": coef,
                    "Param": kwargs.get("param", 0),
                    "PV": kwargs.get("pv", None),
                    "PlayerId": kwargs.get("player_id", 0),
                    "Kind": kwargs.get("kind", 3),
                    "InstrumentId": kwargs.get("instrument_id", 0),
                    "Seconds": kwargs.get("seconds", 0),
                    "Price": kwargs.get("price", 0),
                    "Expired": kwargs.get("expired", 0),
                    "PlayersDuel": kwargs.get("players_duel", [])
                }
            ],
            "Vid": kwargs.get("vid", 0),
            "partner": kwargs.get("partner", 8),
            "Group": kwargs.get("group", 1182),
            "live": kwargs.get("live", False),
            "CheckCf": kwargs.get("check_cf", 1),
            "Lng": kwargs.get("lng", "en"),
            "notWait": kwargs.get("not_wait", True),
            "IsPowerBet": kwargs.get("is_power_bet", False),
            "Summ": amount,
            "Source": kwargs.get("source", 55),
            "OneClickBet": kwargs.get("one_click_bet", 1)
        }
        
        # Add optional parameters if provided
        if kwargs.get("bet_guid"):
            payload["betGUID"] = kwargs["bet_guid"]
        if kwargs.get("promo") is not None:
            payload["promo"] = kwargs["promo"]
        
        return payload

    def create_live_bet_payload(self, user_id, game_id, bet_type, coef, amount, bet_guid, **kwargs):
        """Create a live bet payload (simplified one-click format)"""
        return {
            "UserId": user_id,
            "Events": [
                {
                    "GameId": game_id,
                    "Type": bet_type,
                    "Coef": coef,
                    "Param": kwargs.get("param", 0),
                    "PV": kwargs.get("pv", None),
                    "PlayerId": kwargs.get("player_id", 0),
                    "Kind": kwargs.get("kind", 1),  # Default to Kind 1 for live bets
                    "InstrumentId": kwargs.get("instrument_id", 0),
                    "Seconds": kwargs.get("seconds", 0),
                    "Price": kwargs.get("price", 0),
                    "Expired": kwargs.get("expired", 0),
                    "PlayersDuel": kwargs.get("players_duel", [])
                }
            ],
            "Vid": kwargs.get("vid", 0),
            "partner": kwargs.get("partner", 8),
            "Group": kwargs.get("group", 1182),
            "live": True,  # Always true for live bets
            "CheckCf": kwargs.get("check_cf", 1),  # Simple coefficient check
            "Lng": kwargs.get("lng", "en"),
            "notWait": kwargs.get("not_wait", True),
            "betGUID": bet_guid,  # Required for live bets
            "IsPowerBet": kwargs.get("is_power_bet", False),
            "Summ": amount,
            "Source": kwargs.get("source", 55),
            "OneClickBet": kwargs.get("one_click_bet", 1)  # Simple one-click mode
        }

def main():
    """Example usage of the betting system"""
    bet_placer = MelbetBetPlacer()
    
    print("🎯 Melbet Bet Placer")
    print("=" * 50)
    
    # Example 1: Your exact live bet payload
    live_bet_exact = {
        "UserId": 1370022473,
        "Events": [
            {
                "GameId": 649472464,
                "Type": 6,
                "Coef": 1.09,
                "Param": 0,
                "PV": None,
                "PlayerId": 0,
                "Kind": 1,
                "InstrumentId": 0,
                "Seconds": 0,
                "Price": 0,
                "Expired": 0,
                "PlayersDuel": []
            }
        ],
        "Vid": 0,
        "partner": 8,
        "Group": 1182,
        "live": True,
        "CheckCf": 1,
        "Lng": "en",
        "notWait": True,
        "betGUID": "68b303f15e107b3a30d6e435",
        "IsPowerBet": False,
        "Summ": 10,
        "Source": 55,
        "OneClickBet": 1
    }
    
    print("📱 Live Bet Example (Your Exact Payload):")
    print(json.dumps(live_bet_exact, indent=2))
    
    # Uncomment to place the bet:
    result = bet_placer.place_bet(live_bet_exact)
    if result:
        print(f"✅ Live bet placed! ID: {result.get('Value', {}).get('Id')}")
    
    # Example 2: Using the helper function to create live bet
    print("\n" + "=" * 50)
    print("📱 Creating Live Bet with Helper Function:")
    
    live_bet_helper = bet_placer.create_live_bet_payload(
        user_id=1370022473,
        game_id=649472464,
        bet_type=6,
        coef=1.09,
        amount=10,
        bet_guid="68b303f15e107b3a30d6e435",
        kind=1
    )
    
    print("Live bet payload:")
    print(json.dumps(live_bet_helper, indent=2))
    
    # Example 3: Pre-match bet
    print("\n" + "=" * 50)
    print("⚽ Pre-match Bet Example:")
    
    prematch_bet = bet_placer.create_bet_payload(
        user_id=1370022473,
        game_id=646318347,
        bet_type=3,
        coef=1.264,
        amount=10,
        kind=3,
        live=False
    )
    
    print("Pre-match bet payload:")
    print(json.dumps(prematch_bet, indent=2))

if __name__ == "__main__":
    main()
