import pathlib
from typing import Optional, Union, List

import numpy as np
from sqlalchemy import select, desc

import xarray as xr
import pandas as pd

from common.default import MS
from common.enums import RasterFileType, ForecastAreaEnum, ElementTypeEnum
from config.store_config import StoreConfig
from dao.base import BaseDao
from models.coverages import GeoNCFileModel, GeoTifFileModel
from schema.coverage import CoverageVectorSchema
from util.utils import get_position_forecastarea

STORE_ROOT_PATH: str = r'E:\05DATA\01nginx_data\global_surge'
STORE_RELATIVE_PATH: str = ''


class BaseCoverageDao(BaseDao):

    def get_coveage_file(self, raster_type: RasterFileType, area: ForecastAreaEnum, forecast_ts: int, issue_ts: int,
                         **kwargs) -> Union[
        GeoNCFileModel, GeoTifFileModel]:
        """
            根据 预报 | 发布 时间戳 获取对应的 nc | tif 文件信息
        @param raster_type: 栅格图层种类 nc|tif
        @param area:        预报区域
        @param forecast_ts: 预报时间戳
        @param issue_ts:    发布时间戳
        @param kwargs:
        @return:
        """
        session = self.db.session
        switcher_dict = {
            RasterFileType.NETCDF: GeoNCFileModel,
            RasterFileType.GEOTIFF: GeoTifFileModel
        }
        model_cls = switcher_dict.get(raster_type)
        stmt = select(model_cls).where(
            model_cls.area == area.value,
            model_cls.forecast_ts == forecast_ts,
            model_cls.release_ts == issue_ts)
        res = session.execute(stmt).scalar_one_or_none()
        return res

    def get_coveage_file_byparams(self, raster_type: RasterFileType, area: ForecastAreaEnum, issue_ts: int,
                                  **kwargs) -> Union[
        GeoNCFileModel, GeoTifFileModel]:
        """
            根据 预报 | 发布 时间戳 获取对应的 nc | tif 文件信息
        @param raster_type: 栅格图层种类 nc|tif
        @param area:        预报区域
        @param issue_ts:    发布时间戳
        @param kwargs:
        @return:
        """
        session = self.db.session
        switcher_dict = {
            RasterFileType.NETCDF: GeoNCFileModel,
            RasterFileType.GEOTIFF: GeoTifFileModel
        }
        is_max = kwargs.get('is_max', False)
        product_type: ElementTypeEnum = kwargs.get('product_type', ElementTypeEnum.SURGE)
        model_cls = switcher_dict.get(raster_type)
        stmt = select(model_cls).where(
            model_cls.area == area.value,
            model_cls.release_ts == issue_ts,
            model_cls.product_type == product_type.value,
            model_cls.is_contained_max == is_max)
        res = session.execute(stmt).scalar_one_or_none()
        return res

    def get_last_dist_issuetimes(self, raster_type: RasterFileType, area: ForecastAreaEnum, limit_count: int) -> List[
        int]:
        """
            获取指定预报区域的最近几次的产品发布时间
        @param raster_type: 栅格类型-对应数据库中的表
        @param area:        预报区域
        @param limit_count: 长度限制
        @return: 不同的 发布时间戳 (s)
        """
        session = self.db.session
        switcher_dict = {
            RasterFileType.NETCDF: GeoNCFileModel,
            RasterFileType.GEOTIFF: GeoTifFileModel
        }
        model_cls = switcher_dict.get(raster_type)
        """
            SELECT geo_tif_files.release_ts 
            FROM geo_tif_files 
            WHERE geo_tif_files.area = %s GROUP BY geo_tif_files.release_ts DESC ORDER BY geo_tif_files.release_ts 
             LIMIT %s
        """
        stmt = select(model_cls.release_ts).where(model_cls.area == area.value).group_by(
            model_cls.release_ts).order_by(
            desc(model_cls.release_ts)).limit(limit_count)
        list_ts: List[int] = session.execute(stmt).scalars().all()
        # = [res.release_ts for temp in res]
        return list_ts

    def get_coverage_forecast_tslist(self, raster_type: RasterFileType, area: ForecastAreaEnum, issue_ts: int) -> List[
        int]:
        """
            获取指定预报区域的产品发布时间对应的预报时间戳集合
        @param raster_type: 栅格类型-对应数据库中的tab
        @param area:        预报区域
        @param issue_ts:    产品发布时间戳
        @return: 预报时间戳集合(s)
        """
        session = self.db.session
        switcher_dict = {
            RasterFileType.NETCDF: GeoNCFileModel,
            RasterFileType.GEOTIFF: GeoTifFileModel
        }
        model_cls = switcher_dict.get(raster_type)
        """
            SELECT DISTINCT geo_tif_files.forecast_ts 
            FROM geo_tif_files 
            WHERE geo_tif_files.area = :area_1 AND geo_tif_files.release_ts = :release_ts_1 
            ORDER BY geo_tif_files.forecast_ts DESC
        """

        # todo:[-] 24-11-01 注意对于预报时间戳不需要倒叙排列，升序排列
        stmt = select(model_cls.forecast_ts).where(model_cls.area == area.value,
                                                   model_cls.release_ts == issue_ts).distinct(
            model_cls.forecast_ts).order_by(
            model_cls.forecast_ts)
        list_ts: List[int] = session.execute(stmt).scalars().all()
        # = [res.release_ts for temp in res]
        return list_ts
        pass


class CoverageNcDao(BaseCoverageDao):
    def get_url(self, area: ForecastAreaEnum, forecast_ts: int, issue_ts: int,
                product_type: ElementTypeEnum = ElementTypeEnum.SURGE) -> str:
        """
            获取 对应 tif的url地址(前端加载栅格图层使用)
        @param area:
        @param forecast_ts:
        @param issue_ts:
        @param product_type:
        @return:
        """
        host = StoreConfig.get_ip()
        res = self.get_coveage_file(RasterFileType.GEOTIFF, area, forecast_ts, issue_ts, product_type=product_type)
        relative_path: str = res.relative_path
        file_name: str = res.file_name
        url: str = f'{host}/{StoreConfig.get_store_relative_path()}/{relative_path}/{file_name}'
        return url

    def get_file_full_path(self, area: ForecastAreaEnum, forecast_ts: int, issue_ts: int,
                           product_type: ElementTypeEnum = ElementTypeEnum.SURGE) -> str:
        """
            获取读取nc文件的绝对路径
        @param area:
        @param forecast_ts:
        @param issue_ts:
        @param product_type:
        @return:
        """
        file = self.get_coveage_file_byparams(RasterFileType.NETCDF, area, issue_ts,
                                              product_type=product_type)
        file_full_path: str = pathlib.Path(STORE_ROOT_PATH) / file.relative_path / file.file_name
        return file_full_path

    def get_position_vals(self, lat: float, lon: float, issue_ts: int, start_ts: int, end_ts: int) -> Optional[
        CoverageVectorSchema]:
        """
            根据坐标以及产品发布时间获取对应位置的时序数据(169小时预报)
            会根据传入的坐标自动判断属于哪个区域，并读取对应区域的栅格文件
        @param lat:
        @param lon:
        @param issue_ts: 单位(ms)
        @param start_ts: 单位(ms)
        @param end_ts:   单位(ms)
        @return:
        """
        area = get_position_forecastarea(lat, lon)  #
        """根据传入的经纬度获取预报区域枚举"""
        target_position_vector: Optional[CoverageVectorSchema] = None
        # 获取对应的 nc file url
        if area is not ForecastAreaEnum.OTHER:
            file_path: str = self.get_file_full_path(area, issue_ts, issue_ts, product_type=ElementTypeEnum.SURGE_MERGE)
            if pathlib.Path(file_path).is_file():
                ds: xr.Dataset = xr.open_dataset(file_path)
                start_time = np.datetime64(start_ts, "ms")
                end_time = np.datetime64(end_ts, "ms")
                # 通过临近算法获取与当前 lat,lng 最接近的点
                ds: xr.Dataset = ds.sel(latitude=lat, longitude=lon, method='nearest')
                filter_ds: xr.Dataset = ds['h'].sel(Time=slice(start_time, end_time))
                surge_vals: List[float] = [None if np.isnan(temp) else round(float(temp), 2) for temp in
                                           filter_ds.values]
                """预报增水值集合"""

                # TODO:[-] BUG: 注意此处获取 times 应从 filter_ds 中而非原始的 167小时的ds中获取 time
                time_vals: List[int] = filter_ds['Time'].values
                # TODO:[*] 24-12-05 将 dateimte -> ts
                timestamp_vals: List[int] = [int(pd.Timestamp(temp).timestamp() * MS) for temp in time_vals]
                """对应的预报时间戳(单位:ms)"""
                target_position_vector = CoverageVectorSchema(timestamp_list=timestamp_vals, vals=surge_vals)
        else:
            step: int = 60 * 60 * MS
            timestamp_vals: List[int] = [temp for temp in range(start_ts, end_ts, step)]
            """对应的预报时间戳(单位:ms)"""
            surge_vals: List[Optional[float]] = [None] * len(timestamp_vals)
            """预报增水值集合"""

            target_position_vector = CoverageVectorSchema(timestamp_list=timestamp_vals, vals=surge_vals)

        return target_position_vector


class CoverageTifDao(BaseCoverageDao):
    def get_url(self, area: ForecastAreaEnum, forecast_ts: int, issue_ts: int, ) -> str:
        """
            获取对应的tif文件url
        @param area:
        @param forecast_ts:
        @param issue_ts:
        @return:
        """
        host = StoreConfig.get_ip()
        res = self.get_coveage_file(RasterFileType.GEOTIFF, area, forecast_ts, issue_ts)
        relative_path: str = res.relative_path
        file_name: str = res.file_name
        url: str = f'{host}/{StoreConfig.get_store_relative_path()}/{relative_path}/{file_name}'
        return url

    def get_max_url(self, area: ForecastAreaEnum, issue_ts: int, ) -> str:
        """
            获取对应的tif文件url
        @param area:
        @param issue_ts:
        @return:
        """
        host = StoreConfig.get_ip()
        res = self.get_coveage_file_byparams(RasterFileType.GEOTIFF, area, issue_ts, is_max=True)
        relative_path: str = res.relative_path
        file_name: str = res.file_name
        url: str = f'{host}/{StoreConfig.get_store_relative_path()}/{relative_path}/{file_name}'
        return url
