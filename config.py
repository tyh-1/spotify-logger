from pathlib import Path
from dotenv import load_dotenv
import os

DEFAULT_DB_TYPE_LOCAL = "supabase"  # 本地預設
DB_TYPE_GITHUB_ACTIONS = "supabase"  # GitHub Actions 強制

# turso 相關的先註解掉，因為寫入太慢了，目前換成 supabase

# 無論哪種環境都要有的
REQUIRED_ENV_VARS = ["SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET"]

# 本地要有的
REQUIRED_LOCAL_VARS = ["SPOTIFY_REDIRECT_URI"]

# github_actions 要有的
REQUIRED_GITHUB_ACTIONS = ["REFRESH_TOKEN"]

# 有連接 turso 要有的
# REQUIRED_TURSO_VARS = ["TURSO_DB_URL", "TURSO_DB_TOKEN"]

# 有連接 supabase 要有的
REQUIRED_SUPABASE_VARS = ['SUPABASE_PASSWORD']

def get_config(db_type = None):

    # 1. 判斷環境
    is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'
    
    # 2. (本地) 載入 env
    if not(is_github_actions):
        load_dotenv(Path("env/.env"))
        BASE = Path(__file__).resolve().parent

    # 3. 設定共同 config
    config = {
        # spotify
        "client_id": os.getenv("SPOTIFY_CLIENT_ID"),
        "client_secret": os.getenv("SPOTIFY_CLIENT_SECRET"),
        "scopes": ["user-read-recently-played"],   
        "page_limit": 50,

        # turso
        # "turso_db_url": os.getenv("TURSO_DB_URL"),
        # "turso_db_token": os.getenv("TURSO_DB_TOKEN"),

        # supabase
        'supabase_password': os.getenv('SUPABASE_PASSWORD'),

        # env
        "is_cloud": is_github_actions
    }

    # 4. 依環境添加 config
    # github actions
    if is_github_actions:
        config.update({
            "db_type": DB_TYPE_GITHUB_ACTIONS,
            "refresh_token": os.getenv("REFRESH_TOKEN")
        })

    # 本地環境 db_type 優先順序: arg > .env > DEFAULT
    else:
        if db_type is None:
            db_type = os.getenv('DB_TYPE', DEFAULT_DB_TYPE_LOCAL)
        
        # 檢查 db_type
        _check_db_type(db_type)

        config.update({
            "db_type": db_type,
            "redirect_uri": os.getenv("SPOTIFY_REDIRECT_URI"),
            "token_file": BASE / "env" / "token.json",
        })

    # 5. 檢查必要環境變數有沒有缺
    _check_required_env_vars(config['db_type'], is_github_actions)

    # 6. 增加資料庫相關 config
    config.update(_get_db_config(config['db_type']))
    
    return config


def get_db_connection():

    """取得資料庫連線（根據環境），回傳資料庫連線物件 conn"""

    config = get_config()

    if config['use_supabase']:
        print("連線到 supabase")
        from sqlalchemy import create_engine
        DATABASE_URL = f"postgresql://postgres.wmacdeqyonqhpcxbdpzt:{config['supabase_password']}@aws-1-ap-south-1.pooler.supabase.com:6543/postgres"
        engine = create_engine(DATABASE_URL)
        return engine.begin()
    
    # if not config['use_turso']:
    #     print("連線到本地 SQLite")
    #     import sqlite3
    #     return sqlite3.connect(config['sqlite_path'])
    
    # if config['use_embedded']:
    #     print("連線到 Turso (Embedded Replicas)")
    #     from sqlalchemy import create_engine
    #     engine = create_engine(
    #         "sqlite+libsql:///embedded.db",
    #         connect_args={
    #             "auth_token": config['turso_db_token'],
    #             "sync_url": config['turso_db_url'],
    #         },
    #     )
    #     return engine.connect()


    # print("連線到遠端 Turso")
    # from sqlalchemy import create_engine
    # engine = create_engine(
    #     f"sqlite+{config['turso_db_url']}?secure=true",
    #     connect_args={
    #         "auth_token": config['turso_db_token'],
    #     },
    # )
    # return engine.connect()


# ========== get_config() 輔助函數 ===========
def _check_db_type(db_type):
    """ 檢查本地的 db_type 有沒有效 """
    # valid_types = ['sqlite', 'turso', 'turso_embedded', 'supabase']
    valid_types = ['supabase']
    if db_type not in valid_types:
        raise ValueError(
            f"無效的 db_type: '{db_type}'。"
            f"有效選項: {', '.join(valid_types)}"
        )


def _check_required_env_vars(db_type, env_is_github_actions):
    """
    檢查需要的 env 是否都存在
    根據 db_type, 以及執行環境
    """
    missing = []

    for var in REQUIRED_ENV_VARS:
        if not os.getenv(var):
            missing.append(var)
    
    if env_is_github_actions:
        for var in REQUIRED_GITHUB_ACTIONS:
            if not os.getenv(var):
                missing.append(var)
    
    if not env_is_github_actions:
        for var in REQUIRED_LOCAL_VARS:
            if not os.getenv(var):
                missing.append(var)
    
    # if db_type in ['turso', 'turso_embedded']:
    #     for var in REQUIRED_TURSO_VARS:
    #         if not os.getenv(var):
    #             missing.append(var)

    if db_type == "supabase":
        for var in REQUIRED_SUPABASE_VARS:
            if not os.getenv(var):
                missing.append(var)
    
    if missing:
        raise EnvironmentError(
            f"缺少必要的環境變數: {', '.join(missing)}\n"
            f"請檢查 .env 檔案或 GitHub Secrets 設定"
        )


def _get_db_config(db_type):
    """根據 db_type, 回傳對應 dict, 給 config 更新"""

    db_configs = {
        # 'turso': {
        #     'use_turso': True,
        #     'use_embedded': False
        # },
        # 'turso_embedded': {
        #     'use_turso': True,
        #     'use_embedded': True,
        #     'embedded_path': 'spotify_local.db'
        # },
        'supabase': {
            # 'use_turso': False,
            # 'use_embedded': False,
            'use_supabase': True
        }
    }
    return db_configs[db_type]


if __name__ == "__main__":
    print(get_config())