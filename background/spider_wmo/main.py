import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import os
import time
from datetime import datetime

# --- 配置部分 ---
BASE_URL = "https://severeweather.wmo.int/json"
# 数据保存的根目录
SAVE_ROOT_DIR = "./typhoon_data"
# 设置请求头，模拟浏览器访问，防止被拦截
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}
TIME_OUT = 50


# 创建一个带有重试机制的 session
def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,  # 最多重试 5 次
        backoff_factor=1,  # 每次重试间隔时间 1s, 2s, 4s...
        status_forcelist=[500, 502, 503, 504, 404]  # 针对这些错误重试
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def fetch_and_save_typhoon_data():
    """
    执行一次完整的数据抓取和保存流程
    """

    s = get_session()
    try:
        # 1. 创建按当前时间命名的文件夹 (例如: ./typhoon_data/20251124_0800)
        current_time_str = datetime.now().strftime("%Y%m%d_%H%M")
        current_save_dir = os.path.join(SAVE_ROOT_DIR, current_time_str)

        if not os.path.exists(current_save_dir):
            os.makedirs(current_save_dir)
            print(f"[{current_time_str}] 创建目录: {current_save_dir}")

        # 2. 获取活跃台风列表 (tc_inforce.json)
        inforce_url = f"{BASE_URL}/tc_inforce.json"
        print(f"正在获取活跃列表: {inforce_url}")

        resp = s.get(inforce_url, headers=HEADERS, timeout=TIME_OUT)
        resp.raise_for_status()  # 检查请求是否成功
        inforce_data = resp.json()

        # 保存 tc_inforce.json
        with open(os.path.join(current_save_dir, "tc_inforce.json"), 'w', encoding='utf-8') as f:
            json.dump(inforce_data, f, ensure_ascii=False, indent=4)

        # 3. 解析并遍历台风列表
        # inforce_data['inforce'] 是一个列表的列表
        # 根据 fields 描述，索引 0 是 sysid
        tc_list = inforce_data.get("inforce", [])

        if not tc_list:
            print("当前没有活跃的台风。")
            return

        print(f"发现 {len(tc_list)} 个活跃台风，开始获取详情...")

        for tc_info in tc_list:
            # 提取 sysid (code)
            code = tc_info[0]
            name = tc_info[1] if len(tc_info) > 1 else "Unknown"

            if not code:
                continue

            # 4. 获取单个台风详情 (tc_{code}.json)
            detail_url = f"{BASE_URL}/tc_{code}.json"
            print(f"  - 正在获取台风 [{name}] (ID: {code})...")

            try:
                detail_resp = s.get(detail_url, headers=HEADERS, timeout=TIME_OUT)
                if detail_resp.status_code == 200:
                    detail_data = detail_resp.json()

                    # 保存详情 json
                    filename = f"tc_{code}.json"
                    with open(os.path.join(current_save_dir, filename), 'w', encoding='utf-8') as f:
                        json.dump(detail_data, f, ensure_ascii=False, indent=4)
                else:
                    print(f"    获取失败: HTTP {detail_resp.status_code}")
            except Exception as e:
                print(f"    获取台风 {code} 详情时出错: {e}")

        print(f"[{current_time_str}] 本次任务完成。\n")

    except Exception as e:
        print(f"执行过程中发生全局错误: {e}")


# --- 调度执行部分 ---

if __name__ == "__main__":
    print("开始启动台风数据监控脚本 (按 Ctrl+C 停止)...")

    # 立即执行一次
    fetch_and_save_typhoon_data()

    # 简单的每小时循环 (3600秒)
    while True:
        time.sleep(3600)
        fetch_and_save_typhoon_data()
