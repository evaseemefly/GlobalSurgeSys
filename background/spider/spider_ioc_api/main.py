import os
import sys
import time
import requests
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import arrow
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy.orm import Session
# TODO: [-] 25-12-04 NEW 使用 loguru 替代 logging
from loguru import logger

from common.default import DEFAULT_NAME
from conf.settings import SPIDER_OPTIONS
from core.data import StationSurgeRealData
# 导入你的模型和数据库连接工厂
# 假设 DbFactory 在 core.db 中，并且能够提供 Session
from core.db import DbFactory
from models.models import StationRealDataSpecific, StationInfo
from conf.keys import Keys

# TODO: [-] 25-12-04 NEW 引入 LOG_STORE_PATH
from conf.settings import SPIDER_OPTIONS, LOG_STORE_PATH

# --- 1. 从配置中加载参数 ---
API_BASE_URL = SPIDER_OPTIONS.get('api_base_url')
LIMIT_COUNT = SPIDER_OPTIONS.get('limit', 2000)
TIMEOUT = SPIDER_OPTIONS.get('timeout', 30)
SCHEDULER_INTERVAL = SPIDER_OPTIONS.get('scheduler_interval', 1)

HEADERS = {
    'accept': 'application/json',
    'X-API-KEY': Keys.ioc_api_token
}


def init_logger():
    # --- 日志配置区域 - --
    # TODO: [-] 25-12-04 NEW 配置 Loguru
    # 1. 移除默认的控制台输出（如果想自定义格式的话），否则它默认会输出到 stderr
    logger.remove()
    # 2. 添加控制台输出 (带颜色，方便调试)
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    # 3. 添加文件输出 (按月分文件夹 + 每天一个文件)
    # 说明：
    # {time:YYYY-MM} 会自动创建类似 logs/2025-12/ 的文件夹
    # {time:YYYY-MM-DD} 会生成 logs/2025-12/ioc_spider_2025-12-04.log 文件
    # rotation="00:00" 表示每天 0 点生成新文件
    # retention="1 year" 表示日志保留 1 年
    log_file_path = os.path.join(LOG_STORE_PATH, "{time:YYYY-MM}", "ioc_spider_{time:YYYY-MM-DD}.log")

    """
        rotation:
        含义: 日志切割（轮转）策略。
        作用: 指定何时关闭当前文件并创建一个新文件。
        示例:
        "00:00": 每天凌晨 0 点创建一个新日志文件（按天切割）。
        "500 MB": 当文件大小超过 500MB 时切割。
        "1 week": 每周切割一次。
        
        retention="1 year":
        含义: 日志保留策略。
        作用: 自动清理旧日志。loguru 会在启动时检查旧文件，删除超过指定时间的日志。
    """
    logger.add(
        log_file_path,
        rotation="00:00",
        retention="1 year",
        encoding="utf-8",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        # TODO: [-] 25-12-04 NEW 关键修改：开启 enqueue=True 确保多线程/定时任务下的日志完整性
        enqueue=True,
        # 开启错误回溯诊断，方便排查异常
        backtrace=True,
        diagnose=True
    )
    # --------------------


def get_session_with_retries():
    """
    TODO:[*] 25-12-04 此处的意义？
    (工程化优化) 创建带有重试机制的 Session
    防止因网络抖动导致的临时性连接失败
    """
    # Session 会复用底层的 TCP 连接（Keep-Alive）。
    session = requests.Session()
    # backoff_factor=1：
    # 含义：重试之间的等待时间（退避策略）。
    # status_forcelist=[500, 502, 503, 504]：
    # 含义：只有遇到这些状态码时才重试。。
    # 500: 服务器内部错误
    # 502: 网关错误（常见于 Nginx 转发失败）
    # 503: 服务不可用（超载或维护）
    # 504: 网关超时
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


def fetch_and_save_data():
    """
    核心任务逻辑：获取数据 -> 解析 -> 存入数据库
    """
    logger.info("开始执行定时采集任务...")

    # 动态构建 URL
    api_url = f'{API_BASE_URL}?showall=all&limit={LIMIT_COUNT}'

    # 1. 获取 API 数据 (使用带有重试机制的 session)
    try:
        http_session = get_session_with_retries()
        response = http_session.get(api_url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        data_list = response.json()
    except Exception as e:
        logger.error(f"请求 API 失败: {e}")
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
                    # 记录警告日志
                    logger.warning(f"时间解析失败: {time_str}, Code: {item.get('code')}")
                    real_time_arrow = arrow.now()

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

                # 构建字典对象，包含 station_code
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
                logger.error(f"处理单条数据异常: {ex}, Item: {item}")
                continue

        # 4. 批量保存
        if data_list_for_db:
            # TODO: [修改] 使用 StationSurgeRealData 进行批量入库
            # 初始化处理器 (tid=0)
            handler = StationSurgeRealData(tid=0)
            # 调用批量插入，开启覆盖模式 (to_coverage=True)
            # 此方法会自动根据 dt 分表，并根据 station_code 插入数据
            handler.insert_realdata_list(data_list_for_db, to_coverage=True)
            logger.success(f"任务完成，成功处理数据: {len(data_list_for_db)} 条")
        else:
            logger.info("本次未获取到有效数据")

    except Exception as e:
        session.rollback()
        logger.critical(f"任务执行过程发生严重错误: {e}")
    finally:
        session.close()


def start_scheduler():
    """
    配置并启动定时任务
    """

    scheduler = BlockingScheduler()

    # 添加任务：每分钟执行一次
    # (工程化优化) 从配置读取间隔时间
    scheduler.add_job(
        fetch_and_save_data,
        'interval',
        minutes=SCHEDULER_INTERVAL,
        id='ioc_sealevel_job'
    )

    logger.info(f"定时任务调度器已启动 (Interval: {SCHEDULER_INTERVAL} min)，按 Ctrl+C 退出...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("调度器已停止")
    except Exception as e:
        logger.exception(f"程序发生未捕获异常: {e}")
    finally:
        # TODO: [-] 25-12-04 NEW 关键修改：程序退出前强制刷新所有待写入的日志
        logger.complete()


if __name__ == '__main__':
    init_logger()
    # 第一次启动时立即执行一次，确认功能正常
    fetch_and_save_data()

    # 启动调度器
    start_scheduler()
    import time
