from enum import Enum, unique


@unique
class ElementTypeEnum(Enum):
    """
        预报要素种类枚举
    """
    SURGE = 6002
    """预报-增水"""
    SURGE_MAX = 6003
    """预报-最大增水"""


class RasterFileType(Enum):
    """
        场文件类型
    """
    NETCDF = 6102
    """netcdf"""
    GEOTIFF = 6103
    """geotiff"""


@unique
class ExtremumType(Enum):
    """
        极值美剧
    """
    WIND_EXTREMUM = 801
    """风速极值"""

    WIND_MAX = 802
    """风速最大值"""


@unique
class RegionGroupEnum(Enum):
    """
        站点归属(海洋局|水利部)
    """
    HAIYANG = 1001
    """海洋局"""

    SHUILI = 1002
    """水利部"""


@unique
class RunTypeEnmum(Enum):
    """
        执行 task 类型
    """

    DELATY_TASK = 101
    """延时任务"""
    #
    DATAENTRY_STATION_RANGE = 102
    """录入海洋站(时间段)实况"""

    DATAENTRY_SLB_RANGE = 103
    """录入水利部(时间段)实况"""

    DATAENTRY_FUB_RANGE = 104
    """录入浮标(时间段)实况"""

    DELATY_FUB_TASK = 105
    """延时任务"""

    DELATY_SLB_TASK = 106
    """定时处理水利部站点"""

    DELATY_STATIN_DAILY_TASK = 111
    """每日补录海洋站定时任务"""

    DELATY_FUB_DAILY_TASK = 115
    """每日补录FUB定时任务"""

    DELATY_SLB_DAILY_TASK = 116
    """每日补录SLB定时任务"""


@unique
class ForecastAreaEnum(Enum):
    """
        全球预报产品的预报区域
    """
    AMERICA = 5002
    INDIA_OCEAN = 5003
    OCEANIA = 5004
    WNP = 5005
