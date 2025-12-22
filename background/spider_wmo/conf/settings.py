import os


# TODO: [-] 25-12-04 NEW 定义项目根目录和日志存储路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_STORE_PATH = os.path.join(BASE_DIR, 'logs')