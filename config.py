from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path("env/.env"))

BASE = Path(__file__).resolve().parent     # path object.

SPOTIFY = {
    "client_id": os.getenv("SPOTIFY_CLIENT_ID"),
    "client_secret": os.getenv("SPOTIFY_CLIENT_SECRET"),
    "redirect_uri": os.getenv("SPOTIFY_REDIRECT_URI"),
    "scopes": ["user-read-recently-played"],   
    "page_limit": 50,
    "token_file": BASE / "env" / "token.json"
}

# 檢查必要 env
missing = [k for k in ["client_id","client_secret","redirect_uri"] if not SPOTIFY.get(k)]
if missing:
    raise ValueError(f"Missing env vars for Spotify: {', '.join(missing)}")
