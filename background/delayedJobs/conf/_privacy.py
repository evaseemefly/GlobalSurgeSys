# !本文件不上传!

DB = {

    'DB_PWD': 'Nmefc_814'  # 9.79 mysql
    # 'DB_PWD': 'Surge_811'  # nw
    # 'DB_PWD': '123456'  # 本地测试
}

FTP_LIST = {
    # 海洋站实况ftp下载配置
    'STATION_REALDATA': {
        'HOST': '128.5.9.79',
        'PORT': 21,
        'USER': 'nmefc',
        'PWD': 'Nmefc_surge_814',
        'REMOTE_RELATIVE_PATH': r'/home/nmefc/data_test/数据',
        'LOCAL_PATH': r'E:/05DATA/06wind',  # 本地测试
        # 'LOCAL_PATH': r'/data/local_wind_nwp',  # docker中目录
    },
    'SLB_REALDATA': {
        'HOST': '128.5.9.79',
        'PORT': 21,
        'USER': 'nmefc',
        'PWD': 'Nmefc_surge_814',
        'REMOTE_RELATIVE_PATH': r'/NMEFC_FCST_DATA/WIND',
        'LOCAL_PATH': r'E:/05DATA/06wind',  # 本地测试
        # 'LOCAL_PATH': r'/data/local_wind_nwp',  # docker中目录
    }
}
