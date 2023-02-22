# 公共的装饰器

import wrapt
from typing import List


def provide_store_task(list_stations: List[dict]):
    """
        存储下载作业
    :param list_stations:
    :return:
    """

    @wrapt.decorator()
    def wrapper(wrapped, instance, args, kwargs):
        """

        :param wrapped:
        :param instance:
        :param args:
        :param kwargs:
        :return:
        """
        pass
        # TODO:[*] 将 list_stations 相关信息 to -> db
        return wrapped(*args, **kwargs)

    return wrapper
