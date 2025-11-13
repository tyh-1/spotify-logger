from spotify_log import spotify_recent, sqlite_utils
import os, pandas as pd

# 從 api 抓聆聽資料
tok = spotify_recent.get_valid_token()
df, dim = spotify_recent.fetch_recently_played(tok)
df['played_at'] = pd.to_datetime(df['played_at'], errors='coerce') 
file_nm = "data/1111.csv"
if os.path.exists(file_nm): raise FileExistsError
df.to_csv(file_nm)

# 更新到 db
sqlite_utils.create_tables_if_not_exists()
sqlite_utils.insert_data_from_df(df)