# TODO: [-] 25-12-04 NEW 使用 loguru 替代 logging
import os
import sys

from loguru import logger

from conf.settings import LOG_STORE_PATH


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
