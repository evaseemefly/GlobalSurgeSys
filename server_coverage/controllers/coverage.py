from typing import List, Type, Any, Optional, Union
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from geojson import Point

from common.default import MS
from common.enums import RasterFileType, ForecastAreaEnum
from dao.coverage import CoverageNcDao, CoverageTifDao
from models.coverages import GeoTifFileModel, GeoNCFileModel
from schema.coverage import CoverageFileInfoSchema, CoverageVectorSchema
from util.utils import timestamp_sec2ms, normalize_coord

app = APIRouter()


@app.get('/target/url', response_model=str,
         summary="获取对应的 tif|nc 文件的info", )
def get_coverage_info(area: int, forecast_ts: int, issue_ts: int,
                      raster_type_val: int = RasterFileType.GEOTIFF.value) -> Union[
    GeoNCFileModel, GeoTifFileModel]:
    """
        根据预报区域及时间获取对应的url
    @param issue_ts:
    @return:
    """
    coverage_info = None
    raster_type = get_raster_type_from_int(raster_type_val)
    area_enum = get_area_from_int(area)
    forecast_ts = forecast_ts / MS
    issue_ts = issue_ts / MS
    swtich_dao = {
        RasterFileType.NETCDF: CoverageNcDao,
        RasterFileType.GEOTIFF: CoverageTifDao
    }
    dao_cls = swtich_dao.get(raster_type)
    url = dao_cls().get_url(area_enum, forecast_ts, issue_ts)
    return url


@app.get('/target/max/url', response_model=str,
         summary="获取最大增水场对应的 tif|nc 文件的info", )
def get_surgemax_coverage_info(area: int, issue_ts: int,
                               raster_type_val: int = RasterFileType.GEOTIFF.value) -> str:
    raster_type = get_raster_type_from_int(raster_type_val)
    area_enum = get_area_from_int(area)
    issue_ts = issue_ts / MS
    swtich_dao = {
        RasterFileType.NETCDF: CoverageNcDao,
        RasterFileType.GEOTIFF: CoverageTifDao
    }
    dao_cls = swtich_dao.get(raster_type)
    url = dao_cls().get_max_url(area_enum, issue_ts)
    return url
    pass


@app.get('/last/issue/ts_list', response_model=List[int],
         summary="获取指定预报区域的最近几次的产品发布时间", )
def get_coverage_last_issuetimes(area: int, limit_count: int) -> List[int]:
    coverage_info = None
    area = get_area_from_int(area)
    raster_type = RasterFileType.GEOTIFF
    swtich_dao = {
        RasterFileType.NETCDF: CoverageNcDao,
        RasterFileType.GEOTIFF: CoverageTifDao
    }
    dao_cls = swtich_dao.get(raster_type)
    list_issuetimes = dao_cls().get_last_dist_issuetimes(raster_type, area, 5)
    # TODO:[-] 24-11-04 s -> ms
    list_issuetimes: List[int] = [timestamp_sec2ms(s) for s in list_issuetimes]
    return list_issuetimes


@app.get('/target/forecast/ts_list', response_model=List[int],
         summary="获取指定预报区域的产品发布时间对应的预报时间戳集合", )
def get_coverage_forecast_tslist(area: int, issue_ts: int) -> List[int]:
    """

    @param area:
    @param issue_ts: 传入的时间戳为 ms ，需要转换为 s
    @return:
    """
    coverage_info = None
    issue_ts: int = issue_ts / MS
    area = get_area_from_int(area)
    raster_type = RasterFileType.GEOTIFF
    swtich_dao = {
        RasterFileType.NETCDF: CoverageNcDao,
        RasterFileType.GEOTIFF: CoverageTifDao
    }
    dao_cls = swtich_dao.get(raster_type)

    """发布时间戳集合"""
    list_forecast_ts = dao_cls().get_coverage_forecast_tslist(raster_type, area, issue_ts)
    # TODO:[-] 24-11-04 s -> ms
    list_forecast_ts: List[int] = [timestamp_sec2ms(s) for s in list_forecast_ts]
    return list_forecast_ts


@app.get('/position/forecast/surge/list', response_model=Optional[CoverageVectorSchema],
         summary="获取站点的潮位集合(规定起止范围)")
def get_station_forecast_surgelist(lat: float, lon: float, issue_ts: int, start_ts: int, end_ts: int):
    coverage_info = None
    issue_ts: int = issue_ts / MS
    area = ForecastAreaEnum.WNP
    raster_type = RasterFileType.GEOTIFF

    """
        TODO:[-] 24-12-20
         对输入经纬度加入标准化处理
         lon < 0 -> 360+lon
    """
    standard_lat, standard_lng = normalize_coord(lat, lon)

    forecast_options: Optional[CoverageVectorSchema] = CoverageNcDao().get_position_vals(standard_lat, standard_lng,
                                                                                         issue_ts,
                                                                                         start_ts,
                                                                                         end_ts)
    # TODO:[-] 24-11-04 s -> ms

    return forecast_options


def get_raster_type_from_int(val: int) -> RasterFileType:
    try:
        return RasterFileType(val)
    except ValueError:
        raise ValueError(f"No corresponding RasterFileType for value: {val}")


def get_area_from_int(value: int) -> ForecastAreaEnum:
    try:
        return ForecastAreaEnum(value)
    except ValueError:
        raise ValueError(f"No corresponding ForecastAreaEnum for value: {value}")
