from typing import Optional, Union, List

from sqlalchemy import select, desc

from common.enums import RasterFileType, ForecastAreaEnum
from config.store_config import StoreConfig
from dao.base import BaseDao
from models.coverages import GeoNCFileModel, GeoTifFileModel


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
        model_cls = switcher_dict.get(raster_type)
        stmt = select(model_cls).where(
            model_cls.area == area.value,
            model_cls.release_ts == issue_ts,
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
    def get_url(self, area: ForecastAreaEnum, forecast_ts: int, issue_ts: int, ) -> str:
        host = StoreConfig.get_ip()
        res = self.get_coveage_file(RasterFileType.GEOTIFF, area, forecast_ts, issue_ts)
        relative_path: str = res.relative_path
        file_name: str = res.file_name
        url: str = f'{host}/{StoreConfig.get_store_relative_path()}/{relative_path}/{file_name}'
        return url


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
