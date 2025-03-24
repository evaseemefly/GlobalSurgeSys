STORE_OPTIONS = {
    'NWP': {
        'STORE_ROOT_PATH': '/data/local_wind_nwp'
    }
}

STORE_ROOT_PATH = r'/data/local'
"""容器中的绝对地址"""


class StoreConfig:
    # ip = 'http://192.168.0.104:82'
    # ip = 'http://128.5.9.79:82'
    # ip = 'http://localhost:82'
    ip = 'http://192.168.0.109:82'

    store_relative_path: str = 'images/GLOBAL_FORECAST'

    @classmethod
    def get_ip(cls):
        """
            获取存储动态ip
        @return:
        """
        return cls.ip

    @classmethod
    def get_store_relative_path(cls):
        return cls.store_relative_path
