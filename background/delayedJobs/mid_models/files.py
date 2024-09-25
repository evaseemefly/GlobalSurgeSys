import pathlib
from abc import ABC, abstractmethod
from common.enums import ForecastAreaEnum, ElementTypeEnum, FieldFileType, RasterFileType


class ForecastProductFile:
    """
        文件接口
    """

    def __init__(self, area: ForecastAreaEnum,
                 element_type: ElementTypeEnum, file_name: str, relative_path: str, local_root_path: str,
                 remote_root_path: str = None, ):
        """

        :param local_root_path: 本地存储的根目录(容器对应挂载volums根目录)——绝对路径
        :param element_type:  观测要素种类
        :param remote_root_path:    ftp下载的远端对应登录后的路径(相对路径)
        """
        self.area = area
        self.element_type = element_type
        self.file_name = file_name
        self.relative_path = relative_path
        """存储相对路径"""
        self.local_root_path = local_root_path
        """本地根目录"""
        self.remote_root_path = remote_root_path
        """ftp根目录"""

    @property
    def local_full_path(self) -> str:
        """
            本地存储全路径
        :return:
        """
        path = pathlib.Path(self.local_root_path) / self.relative_path / self.file_name
        return str(path)

    @property
    def remote_full_path(self) -> str:
        """
            ftp存储全路径
        :return:
        """
        path = pathlib.Path(self.remote_root_path) / self.relative_path / self.file_name
        return str(path)

    def exists(self):
        """
            判断当前 self.local_full_path 文件是否存在
        :return:
        """
        return pathlib.Path(self.local_full_path).exists()


class ForecastSurgeRasterFile:
    """
        预报增水场文件
    """

    def __init__(self, area: ForecastAreaEnum, element_type: ElementTypeEnum, field_file_type: RasterFileType,
                 file_name: str, relative_path: str, local_root_path: str):
        self.area = area
        self.element_type = element_type
        self.raster_file_type = field_file_type
        """场文件类型"""
        self.file_name = file_name
        """文件名"""
        self.relative_path = relative_path
        """存储相对路径"""
        self.local_root_path = local_root_path
        """本地存储根目录"""

    @property
    def local_full_path(self) -> str:
        """
            本地存储全路径
        :return:
        """
        path = pathlib.Path(self.local_root_path) / self.relative_path / self.file_name
        return str(path)
