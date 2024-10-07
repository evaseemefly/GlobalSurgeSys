import pathlib
from abc import ABC, abstractmethod, abstractproperty
from typing import List, Optional

import arrow

from common.dicts import dict_area
from common.enums import ForecastAreaEnum, ElementTypeEnum, RasterFileType
from core.stores import CoverageStore, SurgeNcStore, SurgeTifStore
from core.transformers import GlobalSurgeTransformer
from mid_models.files import ForecastProductFile
from util.ftp import FtpClient


class IJob(ABC):
    def __init__(self, ftp_client: FtpClient, ts: int, local_root_path: str, area_type: ForecastAreaEnum,
                 remote_root_path: str = None):
        """

        :param ftp_client: ftp客户端
        :param local_root_path: 本地存储的根目录(容器对应挂载volums根目录)——绝对路径
        :param element_type:  观测要素种类
        :param remote_root_path:    ftp下载的远端对应登录后的路径(相对路径)
        """
        self.ts = ts
        self.ftp_client = ftp_client
        self.local_root_path = local_root_path
        self.remote_root_path = remote_root_path
        self.area = area_type

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


class GlobalSurgeJob(IJob):
    """
        全球风暴潮预报 job
    """

    def get_relative_path(self) -> str:
        """
            获取存储的相对路径
            eg: /mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024092300/nc_latlon/WNP
            ->
                                                    WNP/model_output/2024092300/nc_latlon/WNP
        :return:
        """
        # 根据ts获取当前日期
        date_str: str = arrow.get(self.ts).format('yyyy-mm-dd')
        hours_str: str = arrow.get(self.ts).format('HH')
        date_stamp_str = f'{date_str}{hours_str}'
        area_str: str = dict_area.get(self.area)
        relative_path: str = f'{area_str}/model_output/{date_stamp_str}/nc_latlon/{area_str}'
        return relative_path

    def get_local_path(self) -> str:
        """
            本地存储全路径(dir)
        :return:
        """
        return str(pathlib.Path(self.local_root_path) / self.get_relative_path())

    def get_remote_path(self) -> str:
        """
            ftp远端存储全路径(dir)
            eg: /mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024092300/nc_latlon/WNP
        :return:
        """
        return str(pathlib.Path(self.remote_root_path) / self.get_relative_path())

    def get_remote_files(self) -> List[str]:
        """
            获取ftp远端地址的所有文件名
        :return:
        """
        list_files: List[str] = []
        list_files = self.ftp_client.get_nlist(self.get_remote_path())
        return list_files

    def batch_downloads(self) -> List[ForecastProductFile]:
        """
            批量下载远端文件并返回本地文件路径
        :return: 下载后的本地文件路径
        """
        # list_local_fullpath: List[str] = []
        '''本地下载文件全路径'''

        list_file_name: List[str] = self.get_remote_files()
        list_file: List[ForecastProductFile] = []
        '''远端文件名集合'''
        # 批量下载远端文件
        for temp_name in list_file_name:
            temp_remote_full_path: str = str(pathlib.Path(self.get_remote_path()) / temp_name)
            temp_local_full_path: str = str(pathlib.Path(self.get_local_path()) / temp_name)
            try:
                self.ftp_client.down_load_file(temp_local_full_path, temp_remote_full_path)
                # TODO:[*] 24-09-25 2 db
                # list_local_fullpath.append(temp_local_full_path)
                temp_file: ForecastProductFile = ForecastProductFile(self.area, ElementTypeEnum.SURGE, temp_name,
                                                                     self.get_remote_path(), self.local_root_path,
                                                                     self.remote_root_path)
                list_file.append(temp_file)
            except Exception as e:
                # TODO:[*] 24-09-25 此处加入logger
                pass

        return list_file

    def to_do(self) -> None:
        """
            执行 下载 -> 读取转存 -> 写入db操作
        :return:
        """
        # step1: 批量下载文件
        list_source_files: List[ForecastProductFile] = self.batch_downloads()
        """批量下载后的原始文件"""

        # step2: 下载文件标准化并转存为tiff: standard -> transform
        # TODO:[*] 24-10-07 对于最后一个文件中有含有hmax attribute 应对文件名加以区分
        for temp_file in list_source_files:
            # step2-1: 批量下载后的文件生成 store对象
            source_file_store: SurgeNcStore = SurgeNcStore(temp_file, RasterFileType.NETCDF)
            source_raster_type: RasterFileType = RasterFileType.NETCDF
            is_contained_max: bool = source_file_store.is_contained_max()
            """是否包含max属性"""

            source_file_store.to_db()
            # step2-2: 文件提取并转换
            temp_transformer = GlobalSurgeTransformer(temp_file)
            """当前 file 对应的转换器 instance"""
            temp_transformer.read_data()

            out_put_file: Optional[ForecastProductFile] = temp_transformer.out_put()
            """输出的geotiff文件 instance"""

            # TODO:[*] 24-10-07 对于包含 max 属性的 nc需要多加一步处理max->geotiff

            # step2-3:存储 tif文件 store
            out_put_raster_type: RasterFileType = RasterFileType.GEOTIFF
            out_put_store = SurgeTifStore(out_put_file, out_put_raster_type)
            # step3: 将文件信息写入db
            # 将store写入db
            out_put_store.to_db()

        pass

    def batch_store(self) -> None:
        """
            将下载的文件批量处理并写入db
        :return:
        """
        pass
