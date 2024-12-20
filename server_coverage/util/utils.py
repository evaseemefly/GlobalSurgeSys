from typing import Optional, Tuple

from shapely.geometry import Point, Polygon
from common.default import MS
from common.enums import ForecastAreaEnum


def timestamp_sec2ms(val: int) -> int:
    """
        时间戳转换: s -> ms
    @param val:
    @return:
    """
    return val * MS


def timestamp_ms2sec(val: int) -> int:
    """
        时间戳转换: ms -> s
    @param val:
    @return:
    """
    return val / MS


def get_position_forecastarea(lat: float, lng: float) -> Optional[ForecastAreaEnum]:
    """
        根据传入的经纬度判断该经纬度所属预报区域
    @param lat:
    @param lng:
    @return: 所属预报区域
    """
    forecast_area: Optional[ForecastAreaEnum] = None
    OCEANIA_AREA = Polygon([(-55.0, 100), (-55.0, 210.0), (3, 210.0), (3, 100.0)])
    AMERICA_AREA = Polygon([(0, 210), (0, 330), (60, 330), (60, 210.0)])
    INDIA_OCEAN_AREA = Polygon([(-36.0, 20), (-36.0, 100.0), (32, 100.0), (32, 20.0)])
    WNP_AREA = Polygon([(5.0, 100), (5.0, 145.0), (55.0, 145.0), (55.0, 100.0)])

    position = Point((lat, lng))
    if OCEANIA_AREA.contains(position):
        forecast_area = ForecastAreaEnum.OCEANIA
    elif AMERICA_AREA.contains(position):
        forecast_area = ForecastAreaEnum.AMERICA
    elif INDIA_OCEAN_AREA.contains(position):
        forecast_area = ForecastAreaEnum.INDIA_OCEAN
    elif WNP_AREA.contains(position):
        forecast_area = ForecastAreaEnum.WNP
    else:
        forecast_area = ForecastAreaEnum.OTHER
    return forecast_area


def normalize_coord(lat: float, lng: float) -> Tuple[float, float]:
    """
        将输入的经纬度进行标准化，并返回标准化后的 point
    @param lat:
    @param lng:
    @return:
    """
    if lng < 0:
        lng = 360 + lng
    return lat, lng
