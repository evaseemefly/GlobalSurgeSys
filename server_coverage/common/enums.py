from enum import Enum, unique


@unique
class NullEnum(Enum):
    NULL = -1


@unique
class ElementTypeEnum(Enum):
    """
        预报要素种类枚举
    """
    SURGE = 6002
    """预报-逐时增水"""
    SURGE_MAX = 6003
    """预报-最大增水"""
    # TODO:[-] 24-12-03
    SURGE_MERGE = 6004
    """预报-合并后的逐时增水"""


class RasterFileType(Enum):
    """
        场文件类型
    """
    NETCDF = 6102
    """netcdf"""
    GEOTIFF = 6103
    """geotiff"""


@unique
class ForecastAreaEnum(Enum):
    """
        全球预报产品的预报区域
    """
    AMERICA = 5002
    """美国"""
    INDIA_OCEAN = 5003
    """印度洋"""
    OCEANIA = 5004
    """大洋洲"""
    WNP = 5005
    """西北太"""

    OTHER = 5006
    """其他区域"""
