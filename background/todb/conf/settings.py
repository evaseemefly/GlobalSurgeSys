# 数据库的配置，配置借鉴自 django 的 settings 的结构
DATABASES = {
    'default': {
        'ENGINE': 'mysqldb',  # 数据库引擎
        'NAME': 'surge_global_sys',  # 数据库名
        'USER': 'root',  # 账号
        'PASSWORD': 'Nmefc_814',
        # 'HOST': '128.5.10.21',  # HOST
        # 'HOST': '127.0.0.1',  # HOST
        'HOST': '128.5.9.79',  # HOST
        # 访问宿主的mysql服务,
        # 'HOST': 'host.docker.internal',   # TODO:[*] 22-01-20 暂时注释掉，由于路径使用win的路径，暂时不使用docker作为解释器
        # 'POST': 3306,  # 端口
        # 'HOST': 'mysql',  # HOST
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
