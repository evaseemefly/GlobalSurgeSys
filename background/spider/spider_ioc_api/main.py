import time
import requests
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import arrow
from sqlalchemy.orm import Session

from common.default import DEFAULT_NAME
from conf.settings import SPIDER_OPTIONS
from core.data import StationSurgeRealData
# 导入你的模型和数据库连接工厂
# 假设 DbFactory 在 core.db 中，并且能够提供 Session
from core.db import DbFactory
from models.models import StationRealDataSpecific, StationInfo
from conf.keys import Keys

_limit_count = SPIDER_OPTIONS.get('limit')
# 配置 API 信息
API_URL = f'https://api.ioc-sealevelmonitoring.org/v2/sensors?showall=all&limit={_limit_count}'
HEADERS = {
    'accept': 'application/json',
    'X-API-KEY': Keys.ioc_api_token
}


def fetch_and_save_data():
    """
    核心任务逻辑：获取数据 -> 解析 -> 存入数据库
    """
    print(f"[{datetime.now()}] 开始执行任务...")

    # 1. 获取 API 数据
    try:
        response = requests.get(API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data_list = response.json()
    except Exception as e:
        print(f"请求 API 失败: {e}")
        return

    # 2. 数据库操作
    # 注意：这里假设 DbFactory().Session() 返回一个 scoped_session 或 session 实例
    # 如果你的 DbFactory 写法不同，请根据实际情况调整 session 获取方式
    session = DbFactory().Session  # 或者 DbFactory().Session()

    try:
        # TODO: [修改] 准备用于批量入库的数据列表 (List[Dict])
        # 不再存储 ORM 对象 (new_records)，而是存储字典
        data_list_for_db = []

        for item in data_list:
            try:
                """
                                eg:
                                    "{'code': 'kota', 'location': 'Ko Taphao Noi',
                                     'sensorid': 2, 'sensortype': 'prs', 'lastvalue': 3.986,
                                      'lastupdate': '2013-07-25 07:58:02           ', 
                                      'lasttime': '2013-07-25 07:56:00           ', 
                                      'rate': 1, 'samples': 18, 'units': 'M'}"
                """
                # 数据清洗与提取
                # API 返回示例: "2025-11-27 07:00:00           " (注意后面有空格)
                time_str = item.get('lasttime', '').strip()
                sensor_type: str = item.get('sensortype', DEFAULT_NAME)
                if not time_str:
                    continue

                try:
                    # 解析时间，使用 arrow 解析更为方便，且直接用于后续分表逻辑
                    # TODO: [新增] 使用 arrow 解析时间
                    real_time_arrow = arrow.get(time_str, "YYYY-MM-DD HH:mm:ss")
                except Exception:
                    real_time_arrow = arrow.now()
                    print(f"[Warn] 时间解析失败: {time_str}")

                # 3. 构建ORM对象 (StationRealDataSpecific)
                # 映射关系:
                # code -> station_code
                # lastvalue -> surge
                # lasttime -> gmt_realtime

                # 处理可能出现的 None 或非法数值
                val_str = item.get('lastvalue')
                if val_str is None:
                    # 如果没有值，跳过该记录
                    continue

                # 转换为 float，如果失败会被 except 捕获
                surge_val = float(val_str)

                # TODO: [修改] 构建字典对象，包含 station_code
                record_dict = {
                    'station_code': item.get('code'),
                    'surge': surge_val,
                    'dt': real_time_arrow,  # 关键：传递 arrow 对象
                    'ts': int(real_time_arrow.timestamp()),
                    'sensor_type': sensor_type,
                    # 'tid': 0  # tid 已在 StationSurgeRealData 初始化时指定
                }
                data_list_for_db.append(record_dict)

                # 可选：如果需要同时更新 StationInfo (站点基础信息表)
                # check_and_update_station_info(session, item)
            except Exception as ex:
                # 捕获单个循环中的所有异常，防止因为一条脏数据导致整个批次失败
                print(f"[Error] 处理单条数据出错: {ex}, Data: {item}")
                continue

        # 4. 批量保存
        if data_list_for_db:
            # TODO: [修改] 使用 StationSurgeRealData 进行批量入库
            # 初始化处理器 (tid=0)
            handler = StationSurgeRealData(tid=0)

            # 调用批量插入，开启覆盖模式 (to_coverage=True)
            # 此方法会自动根据 dt 分表，并根据 station_code 插入数据
            handler.insert_realdata_list(data_list_for_db, to_coverage=True)

            print(f"任务完成，成功处理数据条数: {len(data_list_for_db)}")
        else:
            print("未获取到有效数据")

    except Exception as e:
        session.rollback()
        print(f"数据库操作发生错误: {e}")
    finally:
        session.close()


def start_scheduler():
    """
    配置并启动定时任务
    """
    scheduler = BlockingScheduler()

    # 添加任务：每分钟执行一次
    scheduler.add_job(fetch_and_save_data, 'interval', minutes=1, id='ioc_sealevel_job')

    print("定时任务调度器已启动，按 Ctrl+C 退出...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    # 第一次启动时立即执行一次，确认功能正常
    fetch_and_save_data()

    # 启动调度器
    start_scheduler()
    import time
