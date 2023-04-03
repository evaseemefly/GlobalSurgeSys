from typing import List, Type, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

# 本项目
from dao.station import StationDao
from dao.region import RegionDao
from models.models import RegionInfo, StationInfo
from schema.station import StationSchema, MixInStationRegionSchema

app = APIRouter()


@app.get('/list', response_model=List[StationSchema],
         response_model_include=['station_code', 'station_name', 'lat', 'lon', 'is_in_common_user', 'rid']

    , summary="获取全部潮位站的基础数据(含经纬度信息)", )
def get_all_station():
    """
        获取所属当前pid的全部region集合
    @param pid:
    @return:
    """
    station_list = []
    station_list = StationDao().get_all_station()
    # elif only_country == False and pid != -1:
    #     region_list = RegionDao().get_all_region(pid)
    # pydantic.error_wrappers.ValidationError: 2 validation errors for RegionSchema
    return station_list


@app.get('/contain/region/', summary='根据传入的code 获取对应的station,region,country 信息',
         response_model=MixInStationRegionSchema)
def get_station_region(code: str):
    """
        根据 code 获取对应站点的 行政区划信息
        - 所属国家
        - 所属区域
        - 位置信息
        - 最后更新时间
        - lat,lon
    :param code:
    :return:
    """
    station: Optional[StationInfo] = StationDao().get_one_station(code)
    # 根据 station.rid 获取 region
    region: Optional[RegionInfo] = RegionDao().get_region_by_id(station.rid)
    country: Optional[RegionInfo] = RegionDao().get_region_by_id(region.pid)
    # 拼接需要返回的字段
    return MixInStationRegionSchema(station_code=station.station_code, station_name=station.station_name,
                                    lat=station.lat, lon=station.lon, rid=station.rid, val_en=region.val_en,
                                    val_ch=region.val_ch, cid=country.id, country_en=country.val_en)
