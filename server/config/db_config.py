class DBConfig:
    """
    DbConfig DB配置类
    :version: 1.4
    :date: 2020-02-11
    """

    driver = 'mysql+mysqldb'
    host = '127.0.0.1'
    port = '3306'
    username = 'root'
    password = '123456'
    database = 'surge_global_sys'
    charset = 'utf8mb4'
    table_name_prefix = ''
    echo = True
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
