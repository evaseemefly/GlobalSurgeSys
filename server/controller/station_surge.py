from typing import List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from dao.station_surge import StationSurgeDao
from models.models import StationRealDataSpecific
from schema.station_status import StationSurgeSchema
from schema.station_surge import SurgeRealDataSchema

app = APIRouter()


@app.get('/one/{station_code},{start_dt},{end_dt}', response_model=List[StationSurgeSchema],
         summary="获取单站的历史潮位集合")
def get_one_station_surges(station_code: str, start_dt: datetime, end_dt: datetime):
    surge_list = StationSurgeDao().get_station_surge_list(station_code, start_dt, end_dt)
    return surge_list


@app.get('surgel/list/recent', response_model=List[SurgeRealDataSchema],
         response_model_include=['station_code', 'gmt_realtime', 'surge', 'tid'], summary="获取距离当前时间最近的所有潮位站的值")
def get_recently_station_surge(now: datetime = datetime.utcnow()):
    """
        获取距离当前时间最近的所有潮位站的值
    :param now:
    :return:
    """
    recent_station_list = StationSurgeDao().get_dist_station_recently_surge_list()
    return recent_station_list
    # return []
