import arrow
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import os
import time
from datetime import datetime
# TODO: 引入 timedelta 用于计算预报时间
from datetime import datetime, timedelta
# TODO: [-] 25-12-04 NEW 引入 loguru
from loguru import logger

from util.logger import init_logger

# --- 配置部分 ---
BASE_URL = "https://severeweather.wmo.int/json"
# 数据保存的根目录
# SAVE_ROOT_DIR = "/Volumes/DRCC_DATA/01DATA/03全球监测系统/wmo_ty_path_list"
SAVE_ROOT_DIR = r"Z:/WMO_TY_PATH"
# 设置请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}
TIME_OUT = 50


def get_session():
    """创建一个带有重试机制的 session"""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504, 404]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def process_and_save_txt(json_data, save_dir, code, name, org):
    """
        # TODO: 新增函数 - 处理 JSON 数据并保存为指定格式的 TXT
    :param json_data:
    :param save_dir:
    :param code:
    :param name:
    :param org:
    :return:
    """
    try:
        # 1. 提取实况路径 (track) - 取最后 3 个
        track_list = json_data.get("track", [])
        # 如果不足3个则取全部，否则取最后3个
        track_subset = track_list[-3:] if len(track_list) >= 3 else track_list

        # 2. 提取预报路径 (forecast) - 取全部
        forecast_list = json_data.get("forecast", [])

        lines = []

        # TODO: 修改编号格式逻辑 (2025206 -> 25206)
        # 暂时还是用 sysid
        # 如果 code 以 '20' 开头且长度足够，去掉前两位
        # if code and code.startswith("20") and len(code) > 2:
        #     formatted_code = code[2:]
        # else:
        #     formatted_code = code
        formatted_code = code

        # 写入头部信息: 编号, 实况数量, 预报数量
        lines.append(f"{formatted_code}")
        lines.append(f"{len(track_subset) - 1}")
        lines.append(f"{len(forecast_list) + 1}")

        last_analysis_time = None

        # 3. 处理实况数据
        for item in track_subset:
            # 时间格式转换: 2025-11-25 12:00:00 -> MMDDhh
            raw_time = item.get("analysis_time")
            dt = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
            last_analysis_time = dt  # 记录最后一个实况时间，用于计算预报时间

            time_str = dt.strftime("%m%d%H")
            lat = item.get("lat")
            lng = item.get("lng")
            pressure = item.get("pressure")
            # TODO: 25-12-04 获取 max_wind_speed
            max_wind_speed = item.get("max_wind_speed")

            # TODO: 25-12-04 将 max_wind_speed 追加至最后一列
            lines.append(f"{time_str} {lat} {lng} {pressure} {max_wind_speed}")

        # 4. 处理预报数据
        if last_analysis_time and forecast_list:
            for item in forecast_list:
                # 计算预报时间
                # time_interval 是 "1200" 这种格式 (小时 * 100)
                interval_raw = item.get("time_interval")
                if interval_raw:
                    hours_offset = int(interval_raw) / 100
                    forecast_dt = last_analysis_time + timedelta(hours=hours_offset)
                    time_str = forecast_dt.strftime("%m%d%H")
                else:
                    time_str = "000000"  # 异常兜底

                lat = item.get("lat")
                lng = item.get("lng")
                pressure = item.get("pressure")
                # TODO: 25-12-04 获取 max_wind_speed
                max_wind_speed = item.get("max_wind_speed")

                # TODO: 25-12-04 将 max_wind_speed 追加至最后一列
                lines.append(f"{time_str} {lat} {lng} {pressure} {max_wind_speed}")

        # 5. 生成文件名并保存
        # 格式: {code}_{name}_{org}_{当前时间}.txt
        # current_time_str = datetime.now().strftime("%Y%m%d%H%M%S")
        current_time_str = arrow.Arrow.now().format('YYYYMMDDHH')
        safe_org = org if org else "none"  # 处理空机构名

        filename = f"{code}_{name}_{safe_org}_{current_time_str}.txt"
        file_path = os.path.join(save_dir, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"  [TXT] 已生成路径文件: {filename}")

    except Exception as e:
        print(f"  [TXT] 生成失败: {e}")


def fetch_and_save_typhoon_data():
    """
    执行一次完整的数据抓取和保存流程
    """
    s = get_session()

    try:
        # 1. 创建按当前时间命名的文件夹
        # TODO: 25-12-04 使用 arrow 获取 UTC 时间，并按 yyyy/mm/xxx 结构创建目录
        utc_now = arrow.utcnow()
        year_str = utc_now.format('YYYY')
        month_str = utc_now.format('MM')
        # xxx 为具体的批次时间文件夹，例如 20251204_1200
        batch_folder_name = utc_now.format('YYYYMMDD_HHMM')

        # 拼接完整路径: SAVE_ROOT_DIR/yyyy/mm/yyyyMMdd_HHmm
        current_save_dir = os.path.join(SAVE_ROOT_DIR, year_str, month_str, batch_folder_name)

        # 2. 获取活跃台风列表 (tc_inforce.json)
        inforce_url = f"{BASE_URL}/tc_inforce.json"

        # print(f"正在获取活跃列表: {inforce_url}")
        logger.info(f"正在获取活跃列表: {inforce_url}")

        resp = s.get(inforce_url, headers=HEADERS, timeout=TIME_OUT)
        resp.raise_for_status()
        inforce_data = resp.json()

        # 3. 解析台风列表
        tc_list = inforce_data.get("inforce", [])
        if not tc_list:
            # print("当前没有活跃的台风。")
            logger.warning('当前没有活跃的台风')
            return

        # TODO:[-] 25-12-15 创建指定目录 (修改位置：移至文件写入之前)
        # 只有当存在活跃台风时，才真正创建目录
        if not os.path.exists(current_save_dir):
            os.makedirs(current_save_dir)
            logger.info(f"[{batch_folder_name}] (UTC) 创建目录: {current_save_dir}")

        # 保存 tc_inforce.json
        with open(os.path.join(current_save_dir, "tc_inforce.json"), 'w', encoding='utf-8') as f:
            json.dump(inforce_data, f, ensure_ascii=False, indent=4)

        if not os.path.exists(current_save_dir):
            os.makedirs(current_save_dir)
            # print(f"[{batch_folder_name}] (UTC) 创建目录: {current_save_dir}")
            logger.info(f"[{batch_folder_name}] (UTC) 创建目录: {current_save_dir}")

        logger.info(f"发现 {len(tc_list)} 条活跃台风记录，开始解析并获取详情...")

        for tc_info in tc_list:
            # 根据 fields 定义:
            # index 0: sysid (主编号)
            # index 1: name (名称)
            # index 6: same (其他机构编号，逗号分隔字符串)

            sysid = tc_info[0]
            name = tc_info[1] if len(tc_info) > 1 else "Unknown"
            same_str = tc_info[6] if len(tc_info) > 6 else None

            # 构建当前台风需要请求的所有 code 列表
            target_codes = []
            if sysid:
                target_codes.append(sysid)

            # 解析 same 字段中的额外编号
            if same_str:
                # 移除可能存在的空格并按逗号分割
                extra_ids = [x.strip() for x in same_str.split(',') if x.strip()]
                target_codes.extend(extra_ids)

            # 去重 (保持列表顺序可以使用 dict.fromkeys)
            target_codes = list(dict.fromkeys(target_codes))

            logger.info(f"--- 台风 [{name}] (SysID: {sysid}) 关联编号: {target_codes} ---")

            # 4. 遍历该台风下的所有 code 分别请求详情
            for code in target_codes:
                detail_url = f"{BASE_URL}/tc_{code}.json"
                # print(f"  正在请求详情: {code}")

                try:
                    detail_resp = s.get(detail_url, headers=HEADERS, timeout=TIME_OUT)
                    if detail_resp.status_code == 200:
                        detail_data = detail_resp.json()

                        # --- 提取 GTS 机构编号逻辑 ---
                        # 规则: 截取 gts 字段第一个 '.' 前面的最后四个字符作为机构编号
                        # 样例: "WTPQ50RJTD.20251126.1" -> "WTPQ50RJTD" -> "RJTD"
                        gts_raw = detail_data.get("gts")
                        agency_code = "NONE"

                        if gts_raw and "." in gts_raw:
                            # 拿到点号前的部分
                            pre_dot_str = gts_raw.split('.')[0]
                            # 截取后四位 (通常是机构代码，如 RJTD, BABJ 等)
                            if len(pre_dot_str) >= 4:
                                agency_code = pre_dot_str[-4:]
                            else:
                                agency_code = pre_dot_str
                        elif gts_raw:
                            agency_code = gts_raw  # 如果没有点号，直接使用原值

                        print(f"  [成功] Code: {code} | GTS: {gts_raw} | 机构: {agency_code}")

                        # 保存详情 json
                        # 文件名格式: tc_{code}.json
                        filename = f"tc_{code}.json"
                        file_path = os.path.join(current_save_dir, filename)
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(detail_data, f, ensure_ascii=False, indent=4)

                        # TODO: 调用数据处理函数生成 txt
                        # 传入当前请求到的 detail_data, 保存目录, 当前编号 code, 台风名 name, 机构代码 agency_code
                        process_and_save_txt(detail_data, current_save_dir, code, name, agency_code)

                    else:
                        logger.error(f"  [失败] Code: {code} HTTP {detail_resp.status_code}")

                except Exception as e:
                    logger.error(f"  [错误] 请求 {code} 时发生异常: {e}")
        # TODO: 25-12-04 更新完成提示信息
        logger.info(f"[{batch_folder_name}] (UTC) 本次任务完成。\n")

    except Exception as e:
        logger.error(f"执行过程中发生全局错误: {e}")


if __name__ == "__main__":

    # 初始化 loguru 日志模块
    init_logger()
    logger.info("开始启动台风数据监控脚本 (按 Ctrl+C 停止)...")

    # 立即执行一次
    fetch_and_save_typhoon_data()

    # 每小时循环
    while True:
        time.sleep(3600)
        fetch_and_save_typhoon_data()
