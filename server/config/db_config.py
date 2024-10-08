class DBConfig:
    """
    DbConfig DB配置类
    :version: 1.4
    :date: 2020-02-11
    """

    driver = 'mysql+mysqldb'
    # host = '127.0.0.1'
    # 宿主机的mysql服务
    # host = 'host.docker.internal'
    # 线上环境由于 server 解释器为 docker，通过ifconfig 查看 docker0 的网络地址，通过该地址访问
    # host = '172.17.0.1'
    host = '128.5.9.79'
    port = '3306'
    username = 'root'
    password = 'Nmefc_814'
    database = 'surge_global_sys'
    charset = 'utf8mb4'
    table_name_prefix = ''
    echo = False
    pool_size = 100
    max_overflow = 100
    pool_recycle = 60

    def get_url(self):
        config = [
            self.driver,
            '://',
            self.username,
            ':',
            self.password,
            '@',
            self.host,
            ':',
            self.port,
            '/',
            self.database,
            '?charset=',
            self.charset,
        ]

        return ''.join(config)
