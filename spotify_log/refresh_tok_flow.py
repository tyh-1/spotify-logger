import time, random
import requests
import pandas as pd

import config
from spotify_log.parser import parse_track

CONFIG = config.get_config()
CLIENT_ID     = CONFIG["client_id"]
CLIENT_SECRET = CONFIG["client_secret"]

AUTH_URL  = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"


def refresh_access_token(refresh_token: str):
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    r = requests.post(TOKEN_URL, data=data, auth=auth, timeout=30)
    r.raise_for_status()
    j = r.json()
    j.setdefault("refresh_token", refresh_token)
    return j


def get_spotify_items(url, access_token):
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"},
                     params={"limit": 50}, timeout=30)
    if r.status_code == 401:
        raise PermissionError("401")
    r.raise_for_status()
    j = r.json()
    return j.get("next"), j["items"]


def fetch_recently_played(refresh_token):
    tok = refresh_access_token(refresh_token)
    access_token, refresh_token = tok["access_token"], tok["refresh_token"]
    items, next_url = [], "https://api.spotify.com/v1/me/player/recently-played"
    
    while next_url:
        try:
            print(next_url)
            next_url, batch = get_spotify_items(next_url, access_token)
            df_batch = pd.DataFrame(parse_track(x) for x in batch)
               
            items.append(df_batch)
            time.sleep(random.uniform(0, 0.8))
        
        except:
            pass

    return pd.concat(items)
