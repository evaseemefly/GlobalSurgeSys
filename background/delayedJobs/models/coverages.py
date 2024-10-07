from sqlalchemy.orm import Mapped, declarative_base
from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String
from datetime import datetime
from arrow import Arrow
from common.default import DEFAULT_FK, UNLESS_INDEX, NONE_ID, DEFAULT_CODE, DEFAULT_PATH_TYPE, DEFAULT_PRO, \
    UNLESS_RANGE, DEFAULT_TABLE_NAME, DEFAULT_YEAR, DEFAULT_SURGE, DEFAULT_NAME, DEFAULT_COUNTRY_INDEX, DEFAULT_PATH, \
    DEFAULT_EXT, DEFAULT_ENUM
from models.base_model import IDel, IModel, IIdIntModel, IIssueTime, IForecastTime, IReleaseTime

BaseMeta = declarative_base()


class ICoverageFileModel(BaseMeta):
    __abstract__ = True
    relative_path: Mapped[str] = mapped_column(String(50), default=DEFAULT_PATH)
    file_name: Mapped[str] = mapped_column(String(100), default=DEFAULT_NAME)
    time_dims_len: Mapped[int] = mapped_column(default=0)
    # file_ext: Mapped[str] = mapped_column(String(50), default=DEFAULT_EXT)
    # coverage_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)


class GeoNCFileModel(IDel, IIdIntModel, ICoverageFileModel, IReleaseTime, IModel):
    """

    """
    __tablename__ = 'geo_netcdf_files'

    area: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    """预报区域"""
    product_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    """预报产品类型"""

    is_contained_max: bool = mapped_column(default=False)
    """是否包含max属性"""


class GeoTifFileModel(IDel, IIdIntModel, ICoverageFileModel, IReleaseTime, IForecastTime, IModel):
    """
        转换后的tif文件model
    """
    __tablename__ = 'geo_tif_files'

    area: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    """预报区域"""
    product_type: Mapped[int] = mapped_column(default=DEFAULT_ENUM)
    """预报产品类型"""






class RelaNCTifModel(IIdIntModel):
    """
        nc与tif关联表
    """
    __tablename__ = 'rela_nc_tif'

    nc_file_id: Mapped[int] = mapped_column(default=DEFAULT_FK)
    """tb:geo_netcdf_files 表id"""
    tif_file_id: Mapped[int] = mapped_column(default=DEFAULT_FK)
    """tb:geo_tif_files 表id"""
