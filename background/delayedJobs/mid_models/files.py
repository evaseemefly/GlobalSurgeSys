import pathlib
from abc import ABC, abstractmethod

import arrow

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

    @abstractmethod
    def get_issue_ts(self) -> int:
        """获取发布时间"""
        pass

    @abstractmethod
    def get_forecast_ts(self) -> int:
        """获取预报时间"""
        pass


class ForecastSurgeRasterFile(ForecastProductFile):
    """
        预报增水场文件
    """

    def __init__(self, area: ForecastAreaEnum, element_type: ElementTypeEnum, field_file_type: RasterFileType,
                 file_name: str, relative_path: str, local_root_path: str):
        super().__init__(area, element_type, file_name, relative_path, local_root_path)
        # self.area = area
        # self.element_type = element_type
        # self.raster_file_type = field_file_type
        # """场文件类型"""
        # self.file_name = file_name
        # """文件名"""
        # self.relative_path = relative_path
        # """存储相对路径"""
        # self.local_root_path = local_root_path
        # """本地存储根目录"""

    @property
    def local_full_path(self) -> str:
        """
            本地存储全路径
        :return:
        """
        path = pathlib.Path(self.local_root_path) / self.relative_path / self.file_name
        return str(path)

    def get_issue_ts(self) -> int:
        """
            根据存储目录获得发布时间
            path: /mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024092312
               -> 2024092312
        :return:
        """
        path = pathlib.Path(self.relative_path)
        last_dir_name: str = path.parent.name if path.is_file() else path.name
        """eg:2024092312"""
        ts: int = arrow.get(last_dir_name, 'YYYYMMDD-HH').int_timestamp
        return ts

    def get_forecast_ts(self) -> int:
        """
            根据 file_name 生成发布时间
            file_name: field_2024-09-29_12_00_00.f0_WNP_standard_deflate.nc
                    -> 2024-09-29_12_00
        :return:
        """
        file_name: str = self.file_name
        # 获取时间戳
        file_name.split('.')
        pass
