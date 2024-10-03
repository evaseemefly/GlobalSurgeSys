from abc import ABC, abstractmethod

from sqlalchemy.orm import Session

from common.default import NONE_ARGS
from common.enums import RasterFileType
from common.exceptions import CoverageStoreError
from db.db import session_yield_scope
from mid_models.files import ForecastProductFile
from models.coverages import GeoNCFileModel, GeoTifFileModel


class IStore(ABC):

    def __init__(self):
        # self.session = DbFactory().Session
        """数据库session"""
        pass

    @abstractmethod
    def to_db(self, **kwargs):
        """
            写入数据库
        :return:
        """
        pass


class CoverageStore(IStore):
    """
        栅格存储
    """

    def __init__(self, file: ForecastProductFile, raster_type: RasterFileType):
        super().__init__()
        self.file: ForecastProductFile = file
        """当前的栅格文件"""
        self.raster_type = raster_type
        """当前的栅格类型"""

    def to_db(self, **kwargs):
        # 根据文件名称获取对应的预报时间(或从读取信息获取对应的发布与预报时间)
        issue_dt = kwargs.get('issue_dt', NONE_ARGS)
        issue_ts = kwargs.get('issue_ts', NONE_ARGS)
        forecast_dt = kwargs.get('forecast_dt', NONE_ARGS)
        forecast_ts = kwargs.get('forecast_ts', NONE_ARGS)
        with session_yield_scope() as session:
            try:
                # step1: 直接将 raster file 文件信息写入 db
                self.raster_2_db(self.file, self.raster_type, issue_dt, issue_ts, forecast_dt, forecast_ts, session)
                pass
            except Exception as e:
                raise CoverageStoreError()
        pass

    def raster_2_db(self, file: ForecastProductFile, raster_type: RasterFileType, issue_dt, issue_ts, forecast_dt,
                    forecast_ts, session: Session) -> None:
        """
            栅格文件信息写入db
        :param file: 栅格文件
        :param raster_type: 栅格文件类型
        :param issue_dt: 发布时间
        :param issue_ts: 发布时间戳
        :param forecast_dt: 预报时间
        :param forecast_ts: 预报时间戳
        :param session: db session
        :return:
        """
        switcher_dict = {
            RasterFileType.NETCDF: GeoNCFileModel,
            RasterFileType.GEOTIFF: GeoTifFileModel
        }
        """file model switcer选择器"""

        try:
            temp_file_model = switcher_dict.get(raster_type)
            temp_file_model.file_name = file.file_name
            temp_file_model.relative_path = file.relative_path
            temp_file_model.area = file.area
            temp_file_model.product_type = file.element_type
            temp_file_model.forecast_time = forecast_dt
            temp_file_model.forecast_ts = forecast_ts
            temp_file_model.release_ts = issue_ts
            temp_file_model.release_time = issue_dt
            """发布时间+预报时间"""
            session.add(temp_file_model)
            session.commit()

        except Exception as e:
            raise CoverageStoreError

        pass
