import os

from conf.keys import Passwords

# TODO: [-] 25-12-04 NEW 定义项目根目录和日志存储路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_STORE_PATH = os.path.join(BASE_DIR, 'logs')

# 数据库的配置，配置借鉴自 django 的 settings 的结构
DATABASES = {
    'default': {
        'ENGINE': 'pymysql',  # 数据库引擎 TODO:[-] 25-12-01 mac 的 mysql 引擎
        'NAME': 'surge_global_sys',  # 数据库名
        'USER': 'root',  # 账号
        'PASSWORD': Passwords.mysql_pwd,
        # 'HOST': '128.5.9.79',  # HOST
        'HOST': '172.16.30.156',  # HOST
        'POST': 3306,  # 端口
        'OPTIONS': {
            "init_command": "SET foreign_key_checks = 0;",
        },
    },

    'mongo': {
        'NAME': 'wave',  # 数据库名
        'USER': 'root',  # 账号
        'PASSWORD': '123456',
        'HOST': 'localhost',  # HOST
        'POST': 27017,  # 端口
    }
}

TASK_OPTIONS = {
    'name_prefix': 'TASK_SPIDER_GLOBAL_',
    'interval': 10,  # 单位min
}

DB_TABLE_SPLIT_OPTIONS = {
    'station': {
        'tab_split_name': 'station_realdata_specific_ioc'
    }
}

SPIDER_OPTIONS = {
    # API 基础路径
    'api_base_url': 'https://api.ioc-sealevelmonitoring.org/v2/sensors',
    # 每次请求的数据量限制
    'limit': 2000,
    # 请求超时时间 (秒)
    'timeout': 30,
    # 定时任务执行间隔 (分钟)
    'scheduler_interval': 1,
    # (可选优化) 最大重试次数
    'max_retries': 3
}
