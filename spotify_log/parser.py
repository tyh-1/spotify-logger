# Shared parsing utilities. parse_track() is centralized here due to frequent updates.

def parse_track(item):
    t = item["track"]
    return {
        "artist": [a["name"] for a in t["artists"]],
        "artist_id": [a["id"] for a in t["artists"]],
        "track": t["name"],
        "track_id": t["id"],
        "album": t["album"]["name"],
        "album_id": t["album"]["id"],
        "total_tracks": t["album"]["total_tracks"],
        "duration_ms": t["duration_ms"],
        "played_at": item["played_at"],
        "context_type": item.get("context", {}).get("type"),
        "context_uri": item.get("context", {}).get("uri"),
        "track_number": t["track_number"],
        "release_date": t["album"]["release_date"]
    }