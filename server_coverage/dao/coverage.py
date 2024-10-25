from typing import Optional, Union

from sqlalchemy import select

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
