from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from typing import List

from dao.station_status import StationStatusDao
from models.models import StationStatus
from schema.station_status import StationStatusSchema, StationStatusAndGeoInfoSchema

app = APIRouter()


# @app.get('/station/status/all', response_model = List[StationStatusSchema])

@app.get('/all', response_model=List[StationStatusSchema], summary="获取当前全部站点的状态")
def get_all_station_status():
    station_res = StationStatusDao().get_all_station_status()
    # for temp in station_res:
    #     print(temp)
    # pydantic.error_wrappers.ValidationError: 1 validation error for StationStatusSchema
    # response
    #   value is not a valid dict (type=type_error.dict)
    # TODO:[*] 23-03-10 此处存在的问题是 model 与 schema 不能映射
    # list_stations = list(station_res)[:3]
    # temp_station = station_res[0]
    # list_stations = [{
    #     'id': 1,
    #     'station_code': 'aced',
    #     'status': 2,
    #     'tid': 1,
    #     # 'test': 1
    # }, {
    #     'id': 1,
    #     'station_code': 'aced',
    #     'status': 2,
    #     'tid': 1
    # }]
    # pydantic.error_wrappers.ValidationError: 6 validation errors for StationStatusSchema
    # response -> id
    #   field required (type=value_error.missing)
    # ...
    # temp_station_dict = {**temp_station}
    return station_res


@app.get('/one/', response_model=StationStatusSchema, summary="获取单个站点的状态")
def get_one_station_status(station_code: str):
    station_one: StationStatus = StationStatusDao().get_one_station_status(station_code)
    return station_one


@app.get('/all/latlng', response_model=List[StationStatusAndGeoInfoSchema], summary="包含行政区划的全部站点状态")
def get_all_station_join_geoinfo():
    """
         获取所有站点的状态及geo信息(包含surge默认值)
    :return:  {
                "station_code": "waka",
                "status": 1001,
                "gmt_realtime": "2023-04-02T19:29:00",
                "gmt_modify_time": "2023-04-02T19:44:25.022726",
                "is_del": false,
                "lat": 45.41,
                "lon": 141.69,
                "rid": 110,
                "surge": -9999.99
              },
    """
    query = StationStatusDao().get_all_station_status_join_latlng()
    # {'status': 1001, 'gmt_realtime': datetime.datetime(2023, 3, 9, 22, 29), 'gmt_modify_time': datetime.datetime(2023, 3, 9, 22, 46, 23, 406169), 'station_code': 'waka', 'rid': 21, 'lat': 45.41, 'lon': 141.69}
    # 缺少了 is_del
    return query
