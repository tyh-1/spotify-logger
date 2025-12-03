import pandas as pd
from config import get_db_connection
from sqlalchemy import text
import time


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

    # 方便用，不符合 atomic
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS cache (
      artist TEXT[] NOT NULL,   -- array
      artist_id TEXT[] NOT NULL,
      track TEXT NOT NULL,
      track_id TEXT NOT NULL,
      album TEXT,
      album_id TEXT,
      total_tracks INTEGER,
      duration_ms INTEGER NOT NULL,
      played_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
      track_number INTEGER,
      release_date DATE,
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
    df_artists = df_artists.explode(['id', 'artist'])

    df_artists = df_artists[df_artists['id'].astype(bool)]
    df_artists = df_artists[df_artists['artist'].astype(bool)]
    df_artists = df_artists.drop_duplicates(["id"])

    # 5. track_artists. 
    df_track_artists = df[["track_id", "artist_id"]].copy()
    df_track_artists = df_track_artists.explode("artist_id")
    df_track_artists = df_track_artists[df_track_artists["artist_id"].astype(bool)]    # 去除可能的空字串 row
    df_track_artists = df_track_artists.drop_duplicates(["track_id", "artist_id"])
    df_track_artists["artist_order"] = df_track_artists.groupby("track_id").cumcount() + 1

    return {"logs": df_logs, "tracks": df_tracks, "albums": df_albums, "artists": df_artists, "track_artists":df_track_artists}  


def should_update_db(df):
    """
    把新的聆聽紀錄加進 cache table. 當 cahce table 蒐集到一定的量(e.g., 超過 50 筆)，再 flush 進 5 個 tables.
    Return False 或 cache_df
    """
    try:
        df["played_at"] = process_datetime_for_sql(df["played_at"], type = "datetime")
        df["release_date"] = process_datetime_for_sql(df["release_date"], type = "date")
        with get_db_connection() as conn:
            # 讀取整個 cache
            cache = pd.read_sql("SELECT * FROM cache ORDER BY played_at DESC", conn)

            if not cache.empty:
                # 型別轉換、過濾"新資料"
                cache["played_at"] = pd.to_datetime(cache["played_at"])
                cache["release_date"] = pd.to_datetime(cache["release_date"])
                latest_time = cache['played_at'].max()
                new_data = df[df['played_at'] > latest_time].copy()
            else:
                new_data = df.copy()

            if len(new_data) == 0:
                print("無新的聆聽紀錄")
                return False
            
            # 合併新舊資料
            combined = pd.concat([cache, new_data], ignore_index=True)
            
            # 判斷是否達到 flush 門檻
            if combined.shape[0] >= 50: 
                return combined    # 清空快取 insert_date 再處理
            else:
                conn.execute(text("DELETE FROM cache"))
                combined.to_sql("cache", conn, if_exists="append", index=False)
                print(f" Cache 更新：新增 {new_data.shape[0]} 筆，總計 {combined.shape[0]} 筆")
                return False

    except Exception as e:
        print(f"讀寫 cache table 發生錯誤: {e}")
        raise


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
    df = df.sort_values(by='played_at').reset_index(drop=True)
    tables = split_df(df)

    try:
        with get_db_connection() as conn:

            # 先寫 parent table
            for table_name in ["albums", "artists", "tracks", "track_artists", "logs"]:
              start = time.time()
              tables[table_name].to_sql(table_name, conn, if_exists="append", index=False, method = postgres_upsert)
              print(f"   upsert into {table_name}: {time.time()-start:.2f}s")

            start = time.time()
            conn.execute(text("DELETE FROM cache"))
            cache_to_keep = df.nlargest(1, 'played_at')
            cache_to_keep.to_sql("cache", conn, if_exists="append", index=False)
            print(f"更新 cache: {time.time()-start:.2f}s")

    except Exception as e:
        print(f"寫入 {table_name} 發生資料庫錯誤: {e}")
        raise




if __name__ == "__main__":
  pass    