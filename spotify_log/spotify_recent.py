import http.server
import socketserver
import urllib.parse
import webbrowser
import time, random, json, os
import requests
import secrets
import pandas as pd

from config import SPOTIFY

CLIENT_ID     = SPOTIFY["client_id"]
CLIENT_SECRET = SPOTIFY["client_secret"]
REDIRECT_URI  = SPOTIFY["redirect_uri"]
SCOPES        = " ".join(SPOTIFY["scopes"])
STATE         = secrets.token_urlsafe(16)

REDIRECT_URI_PARSE  = urllib.parse.urlparse(REDIRECT_URI)
AUTH_URL  = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
TOKEN_FILE = SPOTIFY["token_file"]


def auth_url():
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": STATE,
        "show_dialog": "true",
    }
    return AUTH_URL + "?" + urllib.parse.urlencode(params)

# ---- 核心：本地 server 用來接住 code ----
class OAuthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != REDIRECT_URI_PARSE.path:
            self.send_response(404)
            self.end_headers()  # header 結束，接下來是 body
            self.wfile.write(b"Not Found")
            return

        qs = urllib.parse.parse_qs(parsed.query)
        if qs.get("state",[""])[0] != STATE: return self.send_error(400,"bad state")
        code = qs.get("code", [None])[0]   # 拿第一個 code，如果沒有就回傳 None

        if code is None:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing 'code' in query.")
            return

        # 把 code 存回 server 物件，主程式等一下要讀
        self.server.auth_code = code
        # self.server.auth_state = state

        # 回應給瀏覽器
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"<html><body><h3>Authorization complete. You can close this window.</h3></body></html>")

    # 靜音 log
    def log_message(self, fmt, *args):
        return

def get_code_via_local_server(timeout=120):
    host = REDIRECT_URI_PARSE.hostname
    port = REDIRECT_URI_PARSE.port
    with socketserver.TCPServer((host, port), OAuthHandler) as httpd:
        # 開瀏覽器去登入授權
        webbrowser.open(auth_url())

        # 持續處理請求直到拿到 code 或 timeout
        deadline = time.time() + timeout
        while not getattr(httpd, "auth_code", None) and time.time() < deadline:
            httpd.handle_request()
        code = getattr(httpd, "auth_code", None)

        # 沒拿到 auth_code, Error
        if not code:
            raise TimeoutError(f"Auth code not received within {timeout} seconds")
        return code


def exchange_code_for_token(code: str):
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI}
    r = requests.post(TOKEN_URL, data=data, auth=auth, timeout=30)
    r.raise_for_status() # 如果是2xx 成功，就不回應；如果像 4xx 等，會丟 error
    return r.json()

def refresh_access_token(refresh_token: str):
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    r = requests.post(TOKEN_URL, data=data, auth=auth, timeout=30)
    r.raise_for_status()
    j = r.json()
    j.setdefault("refresh_token", refresh_token)
    return j

def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tok = json.load(f)
        return tok
    return None


def save_token(tok):
    with open(TOKEN_FILE, "w") as f:
        json.dump(tok, f)


def get_valid_token():
    tok = load_token()
    if tok:
        # 檢查是否快過期
        if time.time() < tok["got_at"] + tok["expires_in"] - 60:
            return tok  # access_token 還有效
        # 用 refresh_token 拿新的
        new_tok = refresh_access_token(tok["refresh_token"])
        new_tok["got_at"] = time.time()
        save_token(new_tok)
        return new_tok
    else:
        # 沒有舊的，就走完整流程
        tok = fetch_token()
        tok["got_at"] = time.time()
        save_token(tok)
        return tok


def get_spotify_items(url, access_token):
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"},
                     params={"limit": 50}, timeout=30)
    if r.status_code == 401:
        raise PermissionError("401")
    r.raise_for_status()
    j = r.json()
    return j.get("next"), j["items"]

def parse_track(item):
    t = item["track"]
    return {
        "artist": ", ".join(a["name"] for a in t["artists"]),
        "artist_id": ", ".join(a["id"] for a in t["artists"]),
        "track": t["name"],
        "track_id": t["id"],
        "album": t["album"]["name"],
        "album_id": t["album"]["id"],
        "total_tracks": t["album"]["total_tracks"],
        "duration_ms": t["duration_ms"],
        "played_at": item["played_at"],
        "track_number": t["track_number"],
        "release_date": t["album"]["release_date"]
    }

def fetch_token():
    code = get_code_via_local_server()
    tok = exchange_code_for_token(code)
    return tok


def get_spotify_items(url, access_token):
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"},
                     params={"limit": 50}, timeout=30)
    if r.status_code == 401:
        raise PermissionError("401")
    r.raise_for_status()
    j = r.json()
    return j.get("next"), j["items"]


def fetch_recently_played(tok):
    access_token, refresh_token = tok["access_token"], tok.get("refresh_token")
    items, next_url = [], "https://api.spotify.com/v1/me/player/recently-played"
    
    while next_url:
        try:
            print(next_url)
            next_url, batch = get_spotify_items(next_url, access_token)
            df_batch = pd.DataFrame(parse_track(x) for x in batch)
               
            items.append(df_batch)
            time.sleep(random.uniform(0, 0.8))

        except PermissionError:
            tok = refresh_access_token(refresh_token)
            access_token, refresh_token = tok["access_token"], tok["refresh_token"]
            continue

    return pd.concat(items), df_batch.shape



if __name__ == "__main__":
    tok = get_valid_token()
    df, dim = fetch_recently_played(tok)
    file_nm = "data/1107.csv"
    if os.path.exists(file_nm): raise FileExistsError
    df.to_csv(file_nm)
    print(df)
    print(dim)

    

