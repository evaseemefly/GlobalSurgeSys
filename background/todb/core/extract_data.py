#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pathlib
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import arrow
import pandas as pd

from conf.settings import TASK_OPTIONS
from core.data import SpiderTask, GTSSurgeRealData
from schemas.surge import GTSPointSchema, GTSEntiretySchema

# --- 配置区域 ---
# 使用 pathlib.Path 定义路径
BASE_DATA_PATH = Path("/root/Decoded_Data")
# 要提取的最新数据点数量
RECORDS_TO_EXTRACT = 20


# --- 配置区域结束 ---
def round_timestamp_to_nearest_second_bakup(timestamp_str: str) -> str:
    """
    使用 arrow 库将一个 ISO 8601 格式的时间戳字符串四舍五入到最接近的秒。

    Args:
        timestamp_str: 原始时间戳字符串，例如 "2025-08-13T02:15:59.616000+00:00"。

    Returns:
        处理后的时间戳字符串，例如 "2025-08-13T02:16:00+00:00"。
    """
    try:
        arrow_obj = arrow.get(timestamp_str)
        # 2. 直接调用 round() 方法，指定单位为 'second'
        rounded_arrow_obj = arrow_obj.ceil('second')
        # 3. 格式化回标准的 ISO 8601 字符串
        return rounded_arrow_obj.isoformat()
    except (arrow.parser.ParserError, TypeError):
        print(f"警告 (arrow)：无法解析时间戳 '{timestamp_str}'，将保持原样。")
        return timestamp_str


def round_dtstr_to_nearest_second(ts: arrow.Arrow) -> str:
    """
        将 arrow 时间对象四舍五入到最接近的秒。
        原理：给时间戳加上500,000微秒（半秒），
              然后对秒进行向下取整。
    @param ts:
    @return:
    """
    rounded_ts = round_timestamp_to_nearest_second(ts)
    formatted_ts = rounded_ts.format("YYYY-MM-DDTHH:mm:ss")
    return formatted_ts


def round_arrow_to_nearest_second(ts: arrow.Arrow) -> arrow.Arrow:
    """
        将 arrow 时间对象四舍五入到最接近的秒。
        原理：给时间戳加上500,000微秒（半秒），
              然后对秒进行向下取整。
    @param ts:
    @return:
    """
    rounded_ts = round_timestamp_to_nearest_second(ts)
    arrow_utc = arrow.get(rounded_ts)
    return arrow_utc


def round_timestamp_to_nearest_second(ts: arrow.Arrow) -> arrow.Arrow:
    """
        将 arrow 时间对象四舍五入到最接近的秒。
        原理：给时间戳加上500,000微秒（半秒），
              然后对秒进行向下取整。
    @param ts:
    @return:
    """
    rounded_ts = ts.shift(microseconds=500000).floor('second')
    return rounded_ts


def get_latest_sea_level_data(root_dir: Path, now_utc: arrow.Arrow) -> List[GTSEntiretySchema]:
    """
            使用 pandas, arrow 和 pathlib 提取当前日期目录下所有站点的
            最新20条海平面数据。
            eg:
                {'dber': {'station_code': 'dber',
                'sensor_type': 'bwl',
                'source_file': 'dber.bwl.238',
                'data_points':
                [{'timestamp_utc': '2025-08-26T01:15:00', 'sea_level_meters': 5222.706},
                 {'timestamp_utc': '2025-08-26T01:30:00','sea_level_meters': 5222.717},
                 {'timestamp_utc': '2025-08-26T01:45:00', 'sea_level_meters': 5222.724},
                 {'timestamp_utc': '2025-08-26T02:00:00', 'sea_level_meters': 5222.727},
                 {'timestamp_utc': '2025-08-26T02:15:00', 'sea_level_meters': 5222.72},
                 }
    @param root_dir:
    @param now_utc:
    @return:
    """
    try:
        # 1. 使用 arrow 获取当前UTC时间并计算DOY
        day_of_year = now_utc.timetuple().tm_yday
        doy_str = f"{day_of_year:03d}"

        # 2. 使用 pathlib 构建当天的目录路径
        today_dir = root_dir / doy_str

        if not today_dir.is_dir():
            print(f"错误：数据目录 '{today_dir}' 不存在。可能今天的数据尚未生成。", file=sys.stderr)
            return None

        latest_data_collection: List[GTSEntiretySchema] = []

        # 3. 遍历目录下的所有文件
        for file_path in today_dir.iterdir():
            if not file_path.is_file():
                continue

            # 4. 解析文件名
            try:
                parts = file_path.name.split('.')
                if len(parts) != 3:
                    continue
                station_code, sensor_type, _ = parts
            except ValueError:
                continue

            # 5. 使用 pandas 读取整个文件
            try:
                # read_csv 可以智能处理多种空白字符作为分隔符
                # header=None 表示文件没有标题行
                # names 指定列名
                df = pd.read_csv(
                    file_path,
                    sep=r'\s+',  # 正则表达式，匹配一个或多个空白字符
                    header=None,
                    names=['time_val', 'sea_level'],
                    engine='python',  # 使用python引擎以支持复杂的sep
                    comment='*'  # 假设以'*'开头的行为注释行，可以忽略
                )

                # 如果文件为空或只有注释行，df会是空的
                if df.empty:
                    continue

            except Exception as e:
                print(f"警告：读取或解析文件 '{file_path.name}' 时出错: {e}", file=sys.stderr)
                continue

            # 6. 获取最后 N 条记录 (如果不足N条，则获取所有记录)
            latest_records = df.tail(RECORDS_TO_EXTRACT)

            data_points: List[GTSPointSchema] = []
            start_of_year = now_utc.floor('year')

            # 7. 遍历这 N 条记录，并转换为所需的格式
            for index, row in latest_records.iterrows():
                try:
                    time_val = float(row['time_val'])
                    sea_level = float(row['sea_level'])

                    # 使用 arrow 计算精确时间戳
                    time_doy = int(time_val)
                    fraction_of_day = time_val - time_doy
                    total_seconds = (time_doy - 1) * 86400 + fraction_of_day * 86400
                    exact_timestamp = start_of_year.shift(seconds=total_seconds)

                    # TODO:[*] 25-08-28 此处有错，返回的为 arrow.Arrow 而非 int 类型
                    stanard_ts: int = round_arrow_to_nearest_second(exact_timestamp).int_timestamp
                    stanard_dt: datetime = round_arrow_to_nearest_second(exact_timestamp).datetime

                    # data_points.append({
                    #     "timestamp_utc": stanard_ts,
                    #     "sea_level_meters": sea_level
                    # })
                    data_points.append(
                        GTSPointSchema(timestamp_utc=stanard_ts, dt_utc=stanard_dt, sea_level_meters=sea_level))
                except (ValueError, TypeError):
                    # 如果某一行数据格式错误，则跳过该行
                    print(f'[*] 读取{file_path}中的:{time_val}错误！')
                    continue

            # 如果成功处理了任何数据点，则添加到最终集合中
            if data_points:
                # latest_data_collection[station_code] = {
                #     "station_code": station_code,
                #     "sensor_type": sensor_type,
                #     "source_file": file_path.name,
                #     "data_points": data_points
                # }
                latest_data_collection.append(
                    GTSEntiretySchema(station_code=station_code, sensor_type=sensor_type, source_file=file_path.name,
                                      data_points=data_points))
            pass

        return latest_data_collection

    except Exception as e:
        print(f"发生未知错误: {e}", file=sys.stderr)
        return None


if __name__ == "__main__":
    dir_path_str: str = r'/Volumes/upload2surge/Decoded_Data'
    dir_path: pathlib.Path = pathlib.Path(dir_path_str)
    out_put: pathlib.Path = pathlib.Path(r'/Users/evaseemefly/03data/02station') / 'all_station_225.json'
    now_utc: arrow.Arrow = arrow.utcnow()
    """
        {'data_points': [{'sea_level_meters': 4125.162, 'timestamp_utc': '2025-08-26T00:00:00'}], 'sensor_type': 'bwl', 'source_file': 'dch3.bwl.238', 'station_code': 'dch3'}
        {'dber': {'station_code': 'dber', 
                'sensor_type': 'bwl', 
                'source_file': 'dber.bwl.238', 
                'data_points': 
                [{'timestamp_utc': '2025-08-26T01:15:00', 'sea_level_meters': 5222.706}, 
                 {'timestamp_utc': '2025-08-26T01:30:00','sea_level_meters': 5222.717}, 
                 {'timestamp_utc': '2025-08-26T01:45:00', 'sea_level_meters': 5222.724}, 
                 {'timestamp_utc': '2025-08-26T02:00:00', 'sea_level_meters': 5222.727}, 
                 {'timestamp_utc': '2025-08-26T02:15:00', 'sea_level_meters': 5222.72}, 
    
    """
    # step1: 定时批量获取最新的gts数据
    # TODO:[-] 25-08-27 此处由 dict -> json 修改为=> -> schema
    all_stations_latest_data: List[GTSEntiretySchema] = get_latest_sea_level_data(dir_path, now_utc)

    # TODO:[*] 25-08-27 将当前时刻的所有实况写入 db
    # 轮训所有站点并分批写入
    for temp_station in all_stations_latest_data:
        temp_code: str = temp_station.station_code
        temp_realdata: List[GTSPointSchema] = temp_station.data_points
        # step2: 将解析后的gts数据集 task 以及 gts data 写入 db
        now_utc: arrow.Arrow = arrow.Arrow.utcnow()
        date_utc_ymdhm: str = now_utc.format('YYYYMMDDHHmm')
        task_name_prefix: str = TASK_OPTIONS.get('name_prefix')
        task_name: str = f'{task_name_prefix}{date_utc_ymdhm}'
        # step2-1 写入 task
        tid: int = -1
        # task_info = SpiderTask(now_utc, len(all_stations_latest_data), task_name)
        # tid = task_info.to_db()
        # step2-2 写入 realdata
        stationSurge = GTSSurgeRealData(temp_station, tid)
        # step1: 创建分表
        # stationSurge.create_split_tab()
        # step2: 像 station_realdata_specific 表中写入当前实况数据
        stationSurge.insert_realdata_list(to_coverage=True, realdata_list=temp_realdata)
        pass

    if all_stations_latest_data:
        try:
            with open(str(out_put), 'w', encoding='utf-8') as f:
                # 使用 json.dump 将数据写入文件
                # indent=4 使文件格式化，易于阅读
                # ensure_ascii=False 确保中文字符或特殊符号直接写入，而不是转义
                json.dump(all_stations_latest_data, f, indent=4, ensure_ascii=False)
            print(f"数据已成功保存到文件: {str(out_put)}")
        except Exception as e:
            print(f"\n错误：写入JSON文件 '{str(out_put)}' 时失败: {e}")
