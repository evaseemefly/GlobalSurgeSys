import os
import logging
import sys
from datetime import datetime
from conf.settings import LOG_STORE_PATH


def setup_logging():
    """
    配置日志系统：
    1. 创建按月分类的日志目录 (LOG_STORE_PATH/YYYY-MM)
    2. 配置控制台输出 (StreamHandler)
    3. 配置各类文件的输出 (FileHandler)
    """

    # 1. 构建按月动态路径: logs/2025-12/
    current_month = datetime.now().strftime('%Y-%m')
    log_dir = os.path.join(LOG_STORE_PATH, current_month)

    # 如果目录不存在，则创建
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # 2. 定义日志文件路径: logs/2025-12/ioc_spider_2025-12-04.log
    # 每天生成一个新文件，或者你可以固定名字
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_file_path = os.path.join(log_dir, f'ioc_spider_{current_date}.log')

    # 3. 获取 Root Logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 清除已有的 handlers (防止重复打印)
    if logger.hasHandlers():
        logger.handlers.clear()

    # 4. 定义格式器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 5. Handler 1: 输出到控制台
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 6. Handler 2: 输出到文件 (追加模式)
    # encoding='utf-8' 防止中文乱码
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger