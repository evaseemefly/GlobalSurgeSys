from abc import ABC, abstractmethod, abstractproperty
import pathlib
from typing import Optional, List, Dict

import arrow

from common.exceptions import FileFormatError
from util.ftp import FtpClient, SFTPClient
from util.common import get_store_relative_path, get_filestamp, get_calendarday_filestamp, \
    get_store_relative_exclude_day, get_fulltime_stamp, get_local_fulltime_stamp
from common.enums import ElementTypeEnum
from util.decorators import decorator_timer_consuming, decorator_exception_logging


class IFile(ABC):
    """
        文件接口
    """

    def __init__(self, ftp_client: SFTPClient, local_root_path: str, element_type: ElementTypeEnum,
                 remote_root_path: str = None):
        """

        :param ftp_client: ftp客户端
        :param local_root_path: 本地存储的根目录(容器对应挂载volums根目录)——绝对路径
        :param element_type:  观测要素种类
        :param remote_root_path:    ftp下载的远端对应登录后的路径(相对路径)
        """
        self.ftp_client = ftp_client
        self.local_root_path = local_root_path
        self.remote_root_path = remote_root_path
        self.element_type = element_type

    @abstractmethod
    def get_remote_path(self) -> str:
        """
            根据传入的时间戳获取对应的远程路径
        :param ts:
        :return:
        """
        pass

    @abstractmethod
    def get_relative_path(self) -> str:
        """本地与远端的存储相对路径"""
        pass

    @abstractmethod
    def get_local_path(self) -> str:
        """
            根据传入的时间获取对应的本地存储路径
        :param ts:
        :return:
        """
        pass

    @abstractmethod
    def get_file_name(self) -> str:
        pass

    @property
    def local_full_path(self) -> str:
        path = pathlib.Path(self.get_local_path()) / self.get_file_name()
        return str(path)

    @decorator_timer_consuming
    def download(self) -> bool:
        """
            TODO:[*] 24-01-17 需要实现，通过ftp下载指定文件
            注意下载至本地需要考虑若存在本地文件需要覆盖
        :return:
        """
        is_ok: bool = False
        # step1: 判断本地路径是否存在，若不存在则创建指定目录
        if pathlib.Path(self.local_full_path).parent.exists():
            pass
        else:
            pathlib.Path(self.local_full_path).parent.mkdir(parents=True, exist_ok=True)
            # step2: 将ftp远端文件 在相对路径下 下载至本地 local_full_path 中
        is_ok = self.ftp_client.download_file(self.local_full_path, self.get_remote_path(),
                                              self.get_file_name())
        return is_ok
