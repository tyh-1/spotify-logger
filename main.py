from spotify_log import sqlite_utils
import os, pandas as pd
from config import get_config

my_config = get_config()

# 從 api 抓聆聽資料
if not my_config["is_cloud"]:
    from spotify_log import spotify_auth_code_flow
    tok = spotify_auth_code_flow.get_valid_token()
    df = spotify_auth_code_flow.fetch_recently_played(tok)
else:
    from spotify_log import spotify_refresh_tok_flow
    df = spotify_refresh_tok_flow.fetch_recently_played(my_config['refresh_token'])

# 如果在本地，就順便存 csv. 提供 debug 素材
if not my_config["is_cloud"]:
    file_nm = "data/1118.csv"
    if os.path.exists(file_nm): raise FileExistsError
    df.to_csv(file_nm)

# 更新到 db
sqlite_utils.create_tables_if_not_exists()
sqlite_utils.insert_data_from_df(df)