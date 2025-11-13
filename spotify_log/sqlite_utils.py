import sqlite3
import pandas as pd


def create_tables_if_not_exists():

  with sqlite3.connect("spotify_log.db") as conn:
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")

    # parent table: albums
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS albums (
      id TEXT NOT NULL PRIMARY KEY,
      album TEXT,
      total_tracks INT,
      release_date TEXT
    );
    """)

    # parent table: artists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS artists (
      id TEXT NOT NULL PRIMARY KEY,
      artist TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
      id TEXT NOT NULL PRIMARY KEY,
      track TEXT NOT NULL,
      album_id TEXT,
      duration_ms INTEGER NOT NULL,
      track_number INTEGER,
                  
      FOREIGN KEY (album_id) REFERENCES albums(id) ON DELETE CASCADE
    );
    """)

    # junction table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS track_artists (
      track_id TEXT NOT NULL,
      artist_id TEXT NOT NULL,
      artist_order INTEGER NOT NULL,
      
      PRIMARY KEY (track_id, artist_id),
      FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE,
      FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs (
      id INTEGER PRIMARY KEY,
      track_id TEXT NOT NULL,
      played_at TEXT NOT NULL,
                  
      FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE,
      UNIQUE (track_id, played_at)
    );
    """)


def split_df(df: pd.DataFrame):
    """把 df 拆成五個 df: logs, tracks, albums, artists, track_artitsts. 要 insert 進 DB 的"""
    # 1. logs
    df_logs = df[["track_id", "played_at"]].copy()

    # 2. tracks
    df_tracks = df[["track_id", "track", "album_id", "duration_ms", "track_number"]].copy()
    df_tracks = df_tracks.rename(columns = {"track_id": "id"})
    print(df_tracks.columns)
    df_tracks = df_tracks.drop_duplicates(["id"])

    # 3. albums
    df_albums = df[["album_id", "album", "total_tracks", "release_date"]].copy()
    df_albums = df_albums.rename(columns = {"album_id": "id"})
    df_albums = df_albums.drop_duplicates(["id"])

    # 4. artists. 違反 atomic, 先 explode 後，再去除重覆
    df_artists = df[["artist_id", "artist"]].copy()
    df_artists = df_artists.rename(columns = {"artist_id": "id"})
    df_artists["id"] = df_artists["id"].str.split(",").apply(lambda x: [i.strip() for i in x])
    df_artists["artist"] = df_artists["artist"].str.split(",").apply(lambda x: [i.strip() for i in x])
    df_artists = df_artists.explode(['id', 'artist'])

    df_artists = df_artists[df_artists['id'].astype(bool)]
    df_artists = df_artists[df_artists['artist'].astype(bool)]
    df_artists = df_artists.drop_duplicates(["id"])

    # 5. track_artists. 
    df_track_artists = df[["track_id", "artist_id"]].copy()
    df_track_artists["artist_id"] = df_track_artists["artist_id"].str.split(",").apply(lambda x: [i.strip() for i in x])
    df_track_artists = df_track_artists.explode("artist_id")
    df_track_artists = df_track_artists[df_track_artists["artist_id"].astype(bool)]    # 去除可能的空字串 row
    df_track_artists = df_track_artists.drop_duplicates(["track_id", "artist_id"])
    df_track_artists["artist_order"] = df_track_artists.groupby("track_id").cumcount() + 1


    return {"logs": df_logs, "tracks": df_tracks, "albums": df_albums, "artists": df_artists, "track_artists":df_track_artists}


def sqlite_replace_metadata(table, conn, keys, data_iter):
    """
    自定義 method，用於 pandas to_sql 實現 SQLite 的 INSERT OR REPLACE 邏輯
    """
    # 轉換成字典列表，[{col1: val1, col2: val2, ...}, {...}, {...}]
    data = [dict(zip(keys, row)) for row in data_iter]
    
    
    cols_str = ", ".join(keys)   # col1, col2, ...
    values_str = ", ".join(f":{k}" for k in keys)  # :col1, :col2, col3

    sql = f"""
    INSERT OR REPLACE INTO {table.name} ({cols_str}) 
    VALUES ({values_str})
    """
    conn.executemany(sql, data)
    return len(data)

def sqlite_insert_ignore(table, conn, keys, data_iter):
    """
    自定義 method，用於 logs table 實現 INSERT OR IGNORE 邏輯
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    
    cols_str = ", ".join(keys)
    values_str = ", ".join(f":{k}" for k in keys) 

    sql = f"""
    INSERT OR IGNORE INTO {table.name} ({cols_str}) 
    VALUES ({values_str})
    """
    conn.executemany(sql, data)
    return len(data)


def insert_data_from_df(df: pd.DataFrame):
    tables = split_df(df)
    try:
        with sqlite3.connect("spotify_log.db") as conn:
            conn.execute("PRAGMA foreign_keys = ON")

            # 先寫 parent table
            for table_name in ["albums", "artists", "tracks", "track_artists", "logs"]:
              tables[table_name].to_sql(table_name, conn, if_exists="append", index=False, method = sqlite_insert_ignore)

        print(f"成功將 {len(df)} 筆資料寫入 spotify.db")

    except sqlite3.Error as e:
        print(f"寫入 {table_name} 發生資料庫錯誤: {e}")




if __name__ == "__main__":

    # 查看 DB 
    conn = sqlite3.connect("spotify_log.db")
    cursor = conn.cursor()
    table_nm = ["logs", "tracks", "albums", "artists", "track_artists"]

    # 1. count
    for t in table_nm:
       print(t)
       print(cursor.execute(f"SELECT COUNT(*) FROM {t};").fetchone())
    print("----------")

    # 2. 前 5 筆
    for t in table_nm:
      x = (cursor.execute(f"""SELECT * FROM {t}
                          LIMIT 5;""").fetchall())
      for y in x:
          print(f"{y}")
      print("----------")




    