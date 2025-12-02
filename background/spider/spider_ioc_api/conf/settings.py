from conf.keys import Passwords

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
