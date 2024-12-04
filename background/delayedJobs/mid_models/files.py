import pathlib
from abc import ABC, abstractmethod

import arrow

from common.enums import ForecastAreaEnum, ElementTypeEnum, RasterFileType
from conf.settings import CONTAINS_HMAX_COUNT


class IForecastProductFile:
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

    @property
    def issue_dt(self) -> arrow.Arrow:
        """
            -> get_issue_ts -> arrow.Arrow
        :return:
        """
        return arrow.get(self.get_issue_ts())

    @abstractmethod
    def get_forecast_ts(self) -> int:
        """获取预报时间"""
        pass

    @property
    def forecast_dt(self) -> arrow.Arrow:
        """
            -> get_forecast_ts -> arrow.Arrow
        :return:
        """
        return arrow.get(self.get_forecast_ts())

    @abstractmethod
    def is_hmax_file(self) -> bool:
        """
            判断是否为包含hmax的nc文件
            判断条件: forecast_ts -issue_ts >=169 -> True
        :return:
        """
        pass


class ForecastSurgeRasterFile(IForecastProductFile):
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
            24-10-16： /mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024101400/nc_latlon/WNP
        :return:
        """
        # path = pathlib.Path(self.relative_path)
        relavtive_path_str: str = self.relative_path
        # last_dir_name: str = path.parent.name if path.is_file() else path.name
        # TODO:[-] 24-10-16 此处由于路径修改此处重新修改
        ts_str: str = relavtive_path_str.rsplit('/', 3)[1:2][0]
        """eg:2024092312"""
        ts: int = arrow.get(ts_str, 'YYYYMMDDHH').int_timestamp
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
        file_date_stamp: str = file_name.split('.')[0]
        """文件中的包含时间的 截取 str
            eg: field_2024-09-29_12_00_00
        """
        date_stamp_str: str = file_date_stamp[6:]
        """eg: 2024-09-29_12_00_00"""
        # 转换为预报时间 utc 时间
        forecast_dt = arrow.get(date_stamp_str, 'YYYY-MM-DD_HH_mm_ss')
        forecast_ts: int = forecast_dt.int_timestamp
        return forecast_ts

    def is_hmax_file(self) -> bool:
        """
            判断是否为包含hmax的nc文件
            判断条件: forecast_ts -issue_ts >=169 -> True
        :return:
        """
        """
            forecast_ts - issue_ts =169 h
        """
        is_hmax: bool = False
        UNIT_HOUR = 60 * 60
        diff_hours = (self.get_forecast_ts() - self.get_issue_ts()) / UNIT_HOUR
        if diff_hours == CONTAINS_HMAX_COUNT:
            is_hmax = True
        return is_hmax


class ForecastSurgeMergeRasterFile(IForecastProductFile):
    """
        预报增水场——合并所有时刻的（包含hmax）
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
            24-10-16： /mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024101400/nc_latlon/WNP
        :return:
        """
        # path = pathlib.Path(self.relative_path)
        relavtive_path_str: str = self.relative_path
        # last_dir_name: str = path.parent.name if path.is_file() else path.name
        # TODO:[-] 24-10-16 此处由于路径修改此处重新修改
        ts_str: str = relavtive_path_str.rsplit('/', 3)[1:2][0]
        """eg:2024092312"""
        ts: int = arrow.get(ts_str, 'YYYYMMDDHH').int_timestamp
        return ts

    def is_hmax_file(self) -> bool:
        """
            判断是否为包含hmax的nc文件
            判断条件: forecast_ts -issue_ts >=169 -> True
        :return:
        """
        """
            forecast_ts - issue_ts =169 h
        """
        is_hmax: bool = False
        UNIT_HOUR = 60 * 60
        diff_hours = (self.get_forecast_ts() - self.get_issue_ts()) / UNIT_HOUR
        if diff_hours == CONTAINS_HMAX_COUNT:
            is_hmax = True
        return is_hmax
