# !本文件不上传!

DB = {

    'DB_PWD': ''  # 9.79 mysql
}

FTP_LIST = {
    # 海洋站实况ftp下载配置
    'STATION_REALDATA': {
        'HOST': '128.5.9.79',
        'PORT': 21,
        'USER': 'nmefc',
        'PWD': '',
        'REMOTE_RELATIVE_PATH': r'/home/nmefc/data_test/数据',
        'LOCAL_PATH': r'E:/05DATA/06wind',  # 本地测试
        # 'LOCAL_PATH': r'/data/local_wind_nwp',  # docker中目录
    },
}
