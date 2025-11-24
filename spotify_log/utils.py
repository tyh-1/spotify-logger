import os
from datetime import datetime

def get_csv_path(base_dir="data"):
    """
    自動生成不重複的 CSV 檔名
    
    Returns:
        str: 完整路徑，例如 "data/1124.csv" 或 "data/1124_2.csv"
    """
    # 1. 確保資料夾存在
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    
    # 2. 生成基礎檔名（今天日期）
    today = datetime.now().strftime("%m%d")  # "1124"
    base_path = os.path.join(base_dir, f"{today}.csv")
    
    # 3. 如果檔案不存在，直接用
    if not os.path.exists(base_path):
        return base_path
    
    # 4. 如果已存在，自動加編號
    counter = 2
    while True:
        new_path = os.path.join(base_dir, f"{today}_{counter}.csv")
        if not os.path.exists(new_path):
            return new_path
        counter += 1