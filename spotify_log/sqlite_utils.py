import pandas as pd
from config import get_db_connection
from sqlalchemy import text


def create_tables_if_not_exists():

   with get_db_connection() as conn:
    
    # parent table: albums
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS albums (
      id TEXT NOT NULL PRIMARY KEY,
      album TEXT,
      total_tracks INT,
      release_date DATE
    );"""))

    # parent table: artists
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS artists (
      id TEXT NOT NULL PRIMARY KEY,
      artist TEXT NOT NULL
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS tracks (
      id TEXT NOT NULL PRIMARY KEY,
      track TEXT NOT NULL,
      album_id TEXT,
      duration_ms INTEGER NOT NULL,
      track_number INTEGER,
                  
      FOREIGN KEY (album_id) REFERENCES albums(id) ON DELETE CASCADE
    );
    """))

    # junction table
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS track_artists (
      track_id TEXT NOT NULL,
      artist_id TEXT NOT NULL,
      artist_order INTEGER NOT NULL,
      
      PRIMARY KEY (track_id, artist_id),
      FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE,
      FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS logs (
      id SERIAL PRIMARY KEY,  
      track_id TEXT NOT NULL,
      played_at TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
      
      FOREIGN KEY (track_id) REFERENCES tracks(id) ON DELETE CASCADE,
      UNIQUE (track_id, played_at)
    );
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS cache (
      track_id TEXT NOT NULL,
      played_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                      
      PRIMARY KEY (track_id, played_at)
    );
    """))


def process_datetime_for_sql(s: pd.Series, type):
    """
    把 datetime 序列，轉換成字串
    parameters:
      s: 要轉成 pd.timestamp 的序列
      type: datetime | date
    """
    if type == "datetime":
      s = pd.to_datetime(s, errors='coerce', utc=True).dt.tz_localize(None).dt.floor('s')
    
    elif type == "date":
      s = pd.to_datetime(s, errors='coerce', format = '%Y-%m-%d')

    else:
      raise ValueError(f"process_datetime_for_sql 函數內，無效的 type argument: {type}. 有效的 argument 為 'datetime' 或 'date'")

    return s

def split_df(df: pd.DataFrame):
    """把 df 拆成五個 df: logs, tracks, albums, artists, track_artitsts. 要 insert 進 DB 的"""
    # 1. logs
    df_logs = df[["track_id", "played_at"]].copy()

    # 2. tracks
    df_tracks = df[["track_id", "track", "album_id", "duration_ms", "track_number"]].copy()
    df_tracks = df_tracks.rename(columns = {"track_id": "id"})
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


def should_update_db(df):
    """
    跟 cache table 比對，看有沒有新的聆聽紀錄，若有 return True.
    """
    df["played_at"] = process_datetime_for_sql(df["played_at"], type = "datetime")


    try:
        with get_db_connection() as conn:
            # 讀取最新一筆
            cache = pd.read_sql(
                "SELECT played_at FROM cache ORDER BY played_at DESC LIMIT 1", 
                conn
            )
  
        if cache.empty: return True  # 第一次執行時，cache table 是空的
        cache_time = pd.to_datetime(cache.loc[0, "played_at"])
        df_time = pd.to_datetime(df.loc[0, "played_at"])
        
        if cache_time == df_time:
            print("No new tracks, skip update")
            return False
        else:
            return True

    except Exception as e:
        print(f"讀取 cache table 發生錯誤: {e}")
        return True


def postgres_upsert(table, conn, keys, data_iter):
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy import MetaData, Table
    
    data_dicts = [dict(zip(keys, row)) for row in data_iter]
    
    if not data_dicts:
        return 0
        
    conflict_columns = {
        'albums': ['id'],
        'artists': ['id'],
        'tracks': ['id'],
        'track_artists': ['track_id', 'artist_id'],
        'logs': ['track_id', 'played_at'],
    }
    
    metadata = MetaData()
    table_obj = Table(table.name, metadata, autoload_with = conn.engine)
    
    stmt = insert(table_obj).values(data_dicts)
    stmt = stmt.on_conflict_do_nothing(
        index_elements = conflict_columns[table.name]
    )
    
    conn.execute(stmt)
    
    return len(data_dicts)


def insert_data_from_df(df: pd.DataFrame):
    
    df["played_at"] = process_datetime_for_sql(df["played_at"], type = "datetime")
    df["release_date"] = process_datetime_for_sql(df["release_date"], type = "date")
    
    tables = split_df(df)
    tables["cache"] = tables["logs"]

    try:
        with get_db_connection() as conn:
            # conn.execute("PRAGMA foreign_keys = ON")

            # 先寫 parent table
            for table_name in ["albums", "artists", "tracks", "track_artists", "logs"]:
              tables[table_name].to_sql(table_name, conn, if_exists="append", index=False, method = postgres_upsert)
              print(f"{table_name} 寫入成功")

            # 清空舊的 cache，插入新的 50 筆
            conn.execute(text("DELETE FROM cache"))
            tables["cache"].to_sql("cache", conn, if_exists="append", index=False)
            print("cache 更新成功")

    except Exception as e:
        print(f"寫入 {table_name} 發生資料庫錯誤: {e}")




if __name__ == "__main__":
  pass    