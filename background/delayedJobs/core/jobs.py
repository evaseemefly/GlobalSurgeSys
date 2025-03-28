import pathlib
from abc import ABC, abstractmethod, abstractproperty
from typing import List, Optional
import xarray as xr
import arrow
from memory_profiler import profile

from common.dicts import dict_area
from common.enums import ForecastAreaEnum, ElementTypeEnum, RasterFileType
from common.logger import log_error, log_warning, log_info
from conf._privacy import FTP_LIST
from core.stores import CoverageStore, SurgeNcStore, SurgeTifStore
from core.transformers import GlobalSurgeTransformer
from mid_models.files import IForecastProductFile, ForecastSurgeRasterFile
from util.ftp import FtpClient, SFTPClient


class IJob(ABC):
    def _init_ftp_client(self) -> SFTPClient:
        """
            初始化 ftp client
        :return:
        """
        ftp_opt = FTP_LIST.get('GLOBAL_SURGE')
        host = ftp_opt.get('HOST')
        port = ftp_opt.get('PORT')
        user_name: str = ftp_opt.get('USER')
        pwd: str = ftp_opt.get('PWD')
        ftp_client = SFTPClient(host, user_name, pwd, port)
        return ftp_client

    def __init__(self, ts: int, local_root_path: str, area_type: ForecastAreaEnum,
                 remote_root_path: str = None):
        """

        :param local_root_path: 本地存储的根目录(容器对应挂载volums根目录)——绝对路径
        :param element_type:  观测要素种类
        :param remote_root_path:    ftp下载的远端对应登录后的路径(相对路径)
        """
        self.ts = ts
        self.ftp_client: SFTPClient = self._init_ftp_client()
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

    @abstractmethod
    def to_do(self) -> None:
        """
            执行 下载 -> 读取转存 -> 写入db操作
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

            # '/mnt/home/nmefc/surge/surge_glb_data/IndiaOcean/WNP/model_output/2024101400/nc_latlon/WNP'
        # 实际 /mnt/home/nmefc/surge/surge_glb_data/IndiaOcean/model_output/2024101400/nc_latlon/IndiaOcean
        :return:
        """
        # 根据ts获取当前日期
        date_str: str = arrow.get(self.ts).format('YYYYMMDD')
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
        # TODO:[-] 24-10-16 注意此处由于存在win环境下运行的可能，而远端地址为linux，此处修改为强制使用linux路径格式
        remote_path: str = str(pathlib.PurePosixPath(self.remote_root_path) / self.get_relative_path())
        return remote_path

    def get_remote_files(self) -> List[str]:
        """
            获取ftp远端地址的所有文件名
        :return:
        """
        list_files: List[str] = []
        list_files = self.ftp_client.get_nlist(self.get_remote_path())
        return list_files

    def batch_downloads(self) -> List[IForecastProductFile]:
        """
            批量下载远端文件并返回本地文件路径
        :return: 下载后的本地文件路径
        """
        # list_local_fullpath: List[str] = []
        '''本地下载文件全路径'''

        list_file_name: List[str] = self.get_remote_files()
        list_file: List[IForecastProductFile] = []
        '''远端文件名集合'''
        # 批量下载远端文件
        for temp_name in list_file_name:

            temp_remote_full_path: str = str(pathlib.PurePosixPath(self.get_remote_path()) / temp_name)
            temp_remote_path: str = self.get_remote_path()
            """远端存储目录"""
            temp_local_path: str = self.get_local_path()
            """本地存储目录"""
            temp_relative_path: str = self.get_relative_path()
            """存储的相对路径"""
            temp_local_full_path: str = str(pathlib.Path(self.get_local_path()) / temp_name)
            try:
                self.ftp_client.download_file(temp_local_path, temp_remote_path, temp_name)
                # TODO:[*] 24-09-25 2 db
                # list_local_fullpath.append(temp_local_full_path)
                temp_file: IForecastProductFile = ForecastSurgeRasterFile(self.area, ElementTypeEnum.SURGE,
                                                                          RasterFileType.NETCDF, temp_name,
                                                                          temp_relative_path,
                                                                          self.local_root_path)
                list_file.append(temp_file)
            except Exception as e:
                # TODO:[*] 24-09-25 此处加入logger
                log_error(f'下载{temp_name}时出错!|msg:{e.args}')
                pass

        return list_file

    @profile
    def to_do(self) -> None:
        """
            执行 下载 -> 读取转存 -> 写入db操作
        :return:
        """
        # TODO:[*] 24-10-14 手动连接与释放连接

        # step1: 批量下载文件
        self.ftp_client.connect()
        list_source_files: List[IForecastProductFile] = self.batch_downloads()
        """批量下载后的原始文件"""
        # 批量写入db
        self.batch_store(list_source_files)

        # TODO:[*] 24-12-04 加入了 nc -> merge.nc 的操作
        self.coverage_merge(list_source_files)
        self.ftp_client.disconnect()
        pass

    def batch_store(self, list_source_files: List[IForecastProductFile]) -> None:
        """
            将下载的文件批量处理并写入db
            TODO:[*] 24-10-10 此处应修改为先查询，若已经存在则更新，若不存在则批量写入
            TODO:[*] 24-12-03 此部分加入对于逐时nc的拼接流程
        :return:
        """
        # step2: 下载文件标准化并转存为tiff: standard -> transform
        # TODO:[*] 24-10-07 对于最后一个文件中有含有hmax attribute 应对文件名加以区分
        for temp_file in list_source_files:
            # step2-1: 批量下载后的文件生成 store对象
            try:
                source_file_store: SurgeNcStore = SurgeNcStore(temp_file, RasterFileType.NETCDF)
                source_raster_type: RasterFileType = RasterFileType.NETCDF
                is_contained_max: bool = source_file_store.is_contained_max()
                """是否包含max属性"""

                source_file_store.to_db(issue_ts=temp_file.get_issue_ts(), issue_dt=temp_file.issue_dt,
                                        forecast_ts=temp_file.get_forecast_ts(), forecast_dt=temp_file.forecast_dt,
                                        is_contained_max=False)
                # step2-2: 文件提取并转换
                temp_transformer = GlobalSurgeTransformer(temp_file)
                """当前 file 对应的转换器 instance"""
                temp_transformer.read_data()

                out_put_file: Optional[IForecastProductFile] = temp_transformer.out_put()
                output_tif_store: SurgeTifStore = SurgeTifStore(out_put_file, RasterFileType.GEOTIFF)
                """输出的geotiff文件 instance"""

                # TODO:[-] 24-12-13 此处修复了包含hmax未能写入db的bug
                output_tif_store.to_db(issue_ts=out_put_file.get_issue_ts(), issue_dt=out_put_file.issue_dt,
                                       forecast_ts=out_put_file.get_forecast_ts(), forecast_dt=out_put_file.forecast_dt,
                                       is_contained_max=False)

                # TODO:[*] 24-10-07 对于包含 max 属性的 nc需要多加一步处理max->geotiff
                if temp_file.is_hmax_file():
                    temp_hmax_file_transoformer = GlobalSurgeTransformer(temp_file)
                    temp_hmax_file_transoformer.read_data('hMax')
                    out_put_file = temp_hmax_file_transoformer.out_put()
                    # step2-3:存储 tif文件 store
                    output_raster_tif_type: RasterFileType = RasterFileType.GEOTIFF
                    output_tif_store = SurgeTifStore(out_put_file, output_raster_tif_type)
                    # step3: 将文件信息写入db
                    # 将store写入db
                    # TODO:[*] 24-10-19 AttributeError: 'RasterFileType' object has no attribute 'get_issue_ts'
                    # 此处加入修改后的包含is_hmax_file 的文件名
                    out_put_file_name: str = out_put_file.file_name
                    output_tif_store.to_db(issue_ts=out_put_file.get_issue_ts(),
                                           issue_dt=out_put_file.issue_dt,
                                           forecast_ts=out_put_file.get_forecast_ts(),
                                           forecast_dt=out_put_file.forecast_dt,
                                           is_contained_max=True, out_put_file_name=out_put_file_name)
            except Exception as e:
                # print(f'{e.args}')
                log_error(f'存储:{temp_file.file_name}时出错!|MSG:{e.args}')
        pass

    def coverage_merge(self, list_source_files: List[IForecastProductFile]) -> None:
        # 创建一个空列表，用于存储每个文件的 Dataset
        datasets = []

        prior_file: Optional[IForecastProductFile] = None
        """发布时间最靠前的file"""

        # 遍历文件列表，读取每个文件的数据
        for file in list_source_files:
            # 若初始文件未被赋值，则指定初始文件
            if prior_file is None:
                prior_file = file
            if prior_file is not None and file.get_issue_ts() < prior_file.get_issue_ts():
                prior_file = file

            file_fullpath: str = file.local_full_path
            # 打开 NetCDF 文件
            ds = xr.open_dataset(file_fullpath)

            # 检查文件中是否包含 h 和 Time
            if "h" in ds.variables and "Time" in ds.dims:
                datasets.append(ds)
            else:
                log_warning(f"文件 {file_fullpath} 中缺少 'h' 变量或 'Time' 维度，跳过处理。")

        # 拼接所有数据集
        if datasets:
            # 使用 xarray.concat 按 Time 维度拼接
            combined_ds = xr.concat(datasets, dim="Time")

            # 打印拼接后的时间数组
            # print("Time 维度的数组：")
            # print(combined_ds["Time"].values)
            log_info(f'当前进行merge合并的dataframe Time维度为:{len(combined_ds["Time"].values)}')

            """
                field_2024-11-05_00_00_00.f0.nc
                -> 
                field_2024-11-05_00_00_00_merge.nc
            """
            temp_file_name: str = f'{prior_file.file_name.split(".")[0]}.merge.nc'

            temp_remote_path: str = self.get_remote_path()
            """远端存储目录"""
            temp_local_path: str = self.get_local_path()
            """本地存储目录"""
            temp_relative_path: str = self.get_relative_path()
            """存储的相对路径"""
            temp_local_full_path: str = str(pathlib.Path(self.get_local_path()) / temp_file_name)

            # 保存拼接后的数据到新的 NetCDF 文件
            combined_ds.to_netcdf(temp_local_full_path)
            # TODO:[*] 24-12-04 获取发布时间的合并文件名

            file_merged: IForecastProductFile = ForecastSurgeRasterFile(self.area, ElementTypeEnum.SURGE_MERGE,
                                                                        RasterFileType.NETCDF, temp_file_name,
                                                                        temp_relative_path,
                                                                        self.local_root_path)

            source_file_store: SurgeNcStore = SurgeNcStore(file_merged, RasterFileType.NETCDF)
            source_file_store.to_db(issue_ts=file_merged.get_issue_ts(), issue_dt=file_merged.issue_dt,
                                    forecast_ts=file_merged.get_forecast_ts(), forecast_dt=file_merged.forecast_dt,
                                    is_contained_max=False)
            # TODO:[*] 24-12-04 print改为 logger
            # print("拼接完成，结果保存为 combined_h.nc")
            log_info("拼接完成，结果保存为 combined_h.nc")
        else:
            # print("未找到可用的数据，未进行拼接。")
            log_warning("拼接完成，结果保存为 combined_h.nc")
