from typing import List, Type, Any, Optional, Union
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from common.enums import RasterFileType, ForecastAreaEnum
from dao.coverage import CoverageNcDao, CoverageTifDao
from models.coverages import GeoTifFileModel, GeoNCFileModel
from schema.coverage import CoverageFileInfoSchema

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
    swtich_dao = {
        RasterFileType.NETCDF: CoverageNcDao,
        RasterFileType.GEOTIFF: CoverageTifDao
    }
    dao_cls = swtich_dao.get(raster_type)
    url = dao_cls().get_url(area_enum, forecast_ts, issue_ts)
    return url


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
