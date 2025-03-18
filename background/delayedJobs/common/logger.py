from loguru import logger
import os

# 创建日志目录
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# 配置日志
logger.add(
    os.path.join(log_directory, "app.log"),  # 日志文件路径
    rotation="1 MB",  # 每 1 MB 轮换日志
    retention="7 days",  # 保留 7 天的日志
    level="DEBUG",  # 记录级别
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",  # 日志格式
)


def log_info(message):
    """记录普通信息级别的日志"""
    logger.info(message)


def log_warning(message):
    """记录警告级别的日志"""
    logger.warning(message)


def log_error(message):
    """记录错误级别的日志"""
    logger.error(message)


def log_exception(exc):
    """记录异常信息"""
    logger.exception(exc)
