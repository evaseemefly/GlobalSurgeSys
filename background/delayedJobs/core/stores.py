from abc import ABC, abstractmethod

import arrow
from sqlalchemy import select, distinct, update
from sqlalchemy.orm import Session

from common.default import NONE_ARGS
from common.enums import RasterFileType
from common.exceptions import CoverageStoreError
from db.db import session_yield_scope
from mid_models.files import IForecastProductFile
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

    def __init__(self, file: IForecastProductFile, raster_type: RasterFileType):
        super().__init__()
        self.file: IForecastProductFile = file
        """当前的栅格文件"""
        self.raster_type = raster_type
        """当前的栅格类型"""

    def raster_2_db(self, file: IForecastProductFile, raster_type: RasterFileType, issue_dt, issue_ts, forecast_dt,
                    forecast_ts, is_contained_max: bool, session: Session) -> None:
        """
            栅格文件信息写入db
            TODO:[*] 24-10-19 此处应修改为根据 forecast_ts + issue_ts + file.area 进行 add 或者 update
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
        """file model switch 选择器"""
        current_dt: arrow.Arrow = arrow.utcnow()
        try:
            model_cls = switcher_dict.get(raster_type)
            temp_file_model = model_cls()
            """
                TODO:[-] 24-10-21 此处加入判断若 forecast_ts + issue_ts + file.area 已存在则直接更新
                ERROR:
                Column expression, FROM clause, or other columns clause element expected, got <models.coverages.GeoNCFileModel object at 0x00000293915CC908>.
            """
            filter_stmt = select(model_cls).where(
                model_cls.area == file.area.value,
                model_cls.forecast_ts == forecast_ts,
                model_cls.release_ts == issue_ts)
            # Multiple rows were found when one or none was required
            filter_res = session.execute(filter_stmt).scalar_one_or_none()
            if filter_res:
                update_stmt = (update(model_cls).where(
                    model_cls.area == file.area.value,
                    model_cls.forecast_ts == forecast_ts,
                    model_cls.release_ts == issue_ts).values(
                    file_name=file.file_name,
                    relative_path=file.relative_path,
                    area=file.area.value,
                    product_type=file.element_type.value,
                    forecast_time=forecast_dt,
                    forecast_ts=forecast_ts,
                    release_ts=issue_ts,
                    release_time=issue_dt,
                    is_contained_max=is_contained_max,
                    gmt_modify_time=current_dt
                ))
                session.execute(update_stmt)
            else:
                temp_file_model.file_name = file.file_name
                temp_file_model.relative_path = file.relative_path
                temp_file_model.area = file.area.value
                temp_file_model.product_type = file.element_type.value
                temp_file_model.forecast_time = forecast_dt
                temp_file_model.forecast_ts = forecast_ts
                temp_file_model.release_ts = issue_ts
                temp_file_model.release_time = issue_dt
                temp_file_model.is_contained_max = is_contained_max
                """发布时间+预报时间"""
                session.add(temp_file_model)
            session.commit()

        except Exception as e:
            """
            """
            raise CoverageStoreError

        pass

    # @abstractmethod
    # def to_db(self, **kwargs):
    #     """
    #         写入db
    #     """
    #     pass

    def to_db(self, **kwargs):
        # 根据文件名称获取对应的预报时间(或从读取信息获取对应的发布与预报时间)
        issue_dt = kwargs.get('issue_dt', NONE_ARGS)
        issue_ts = kwargs.get('issue_ts', NONE_ARGS)
        forecast_dt = kwargs.get('forecast_dt', NONE_ARGS)
        forecast_ts = kwargs.get('forecast_ts', NONE_ARGS)
        is_contained_max: bool = kwargs.get('is_contained_max', NONE_ARGS)
        """是否包含最大值"""

        with session_yield_scope() as session:
            try:
                # step1: 直接将 raster file 文件信息写入 db
                self.raster_2_db(self.file, self.raster_type, issue_dt, issue_ts, forecast_dt, forecast_ts,
                                 is_contained_max, session)
                pass
            except Exception as e:
                raise CoverageStoreError()
        pass


class SurgeNcStore(CoverageStore):
    """
        nc 文件存储器
    """

    def is_contained_max(self) -> bool:
        """
            TODO:[*] 24-10-07 是否包含 max 属性(通过文件名判断)
        @return:
        """
        is_contained = False
        return is_contained
        pass

    pass


class SurgeTifStore(CoverageStore):
    """
        geotiff 文件存储器
    """
    pass
