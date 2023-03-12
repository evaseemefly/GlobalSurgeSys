from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from typing import List

from dao.station_status import StationStatusDao
from models.models import StationStatus
from schema.station_status import StationStatusSchema

app = APIRouter()


# @app.get('/station/status/all', response_model = List[StationStatusSchema])

@app.get('/all', response_model=List[StationStatusSchema])
def get_all_station_status():
    station_res = StationStatusDao().get_all_station_status()
    # for temp in station_res:
    #     print(temp)
    # pydantic.error_wrappers.ValidationError: 1 validation error for StationStatusSchema
    # response
    #   value is not a valid dict (type=type_error.dict)
    # TODO:[*] 23-03-10 此处存在的问题是 model 与 schema 不能映射
    list_stations = list(station_res)[:3]
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
    return list_stations


@app.get('/one/{station_code}', response_model=StationStatusSchema)
def get_one_station_status(station_code: str):
    station_one: StationStatus = StationStatusDao().get_one_station_status(station_code)
    return station_one


