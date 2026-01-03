from spotify_log import db_utils, auth_code_flow
import pandas as pd, time
from config import get_config

my_config = get_config()
print(f"開始執行：{pd.Timestamp.now()}")
start = time.time()

artists_list = db_utils.get_artists_without_genres()
print(f"取得需要新增的 artist 資料: {time.time() - start:.2f}s")
if not artists_list:
    print("所有 artist 都已有 genres")
    exit(0)

start = time.time()
tok = auth_code_flow.get_valid_token()
genres_df = auth_code_flow.fetch_artist_genres(artists_list, tok)
print(f"取得需 artist 的 genres 資料: {time.time() - start:.2f}s")

start = time.time()
db_utils.insert_genres_data(genres_df)
print(f"更新成功: {time.time() - start:.2f}s")