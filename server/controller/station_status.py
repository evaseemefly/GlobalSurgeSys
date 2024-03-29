from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from typing import List

from dao.station_status import StationStatusDao
from models.models import StationStatus
from schema.station_status import StationStatusSchema, StationStatusAndGeoInfoSchema, StationStatusMixRegionchema

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
    # res_dict: list[dict] = []
    # for temp in list(station_res):
    #     temp_dict = {'id': temp.id, 'station_code': temp.station_code, 'status': temp.status, 'is_del': temp.is_del,
    #                  'tid': temp.tid, 'gmt_realtime': temp.gmt_realtime, 'gmt_modify_time': temp.gmt_modify_time}
    #     res_dict.append(temp_dict)
    # res_dict: list[dict] = [
    #     {'station_code': temp['station_code'], 'status': temp['status'], 'is_del': temp['is_del'],
    #      'lat': temp['lat'], 'lon': temp['lon'], 'rid': temp['rid'], 'surge': temp['surge'], } for temp
    #     in
    #     station_res]
    return station_res


@app.get('/one/', response_model=StationStatusSchema, summary="获取单个站点的状态")
def get_one_station_status(station_code: str):
    station_one: StationStatus = StationStatusDao().get_one_station_status(station_code)
    return station_one


@app.get('/all/latlng', response_model=List[StationStatusAndGeoInfoSchema], summary="包含行政区划的全部站点状态")
def get_all_station_join_geoinfo(pid: int = None):
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
    query = StationStatusDao().get_all_station_status_join_latlng(pid)
    # {'status': 1001, 'gmt_realtime': datetime.datetime(2023, 3, 9, 22, 29), 'gmt_modify_time': datetime.datetime(2023, 3, 9, 22, 46, 23, 406169), 'station_code': 'waka', 'rid': 21, 'lat': 45.41, 'lon': 141.69}
    # 缺少了 is_del
    return query


@app.get('/all/station_status/pid/', response_model=List[StationStatusMixRegionchema],
         summary="根据 pid 获取指定区域的所有站点的最后更新时间")
def get_all_station_updatedt_by_pid(pid: int):
    query = StationStatusDao().get_all_station_updatedate_by_pid(pid)
    return query
