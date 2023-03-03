# 公共的装饰器

import wrapt
import time
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


def timer_count(num: int):
    """
        方法耗时计算装饰器
        参考:
        https://zhuanlan.zhihu.com/p/68579288
        https://blog.csdn.net/weixin_40759186/article/details/86472644
    """

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        old_time = time.time()
        res = wrapped(*args, **kwargs)
        new_time = time.time()
        print(f'执行func:{wrapped},耗时{new_time - old_time}')
        return res

    return wrapper
