from typing import List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from dao.station_surge import StationSurgeDao
from models.models import StationRealDataSpecific
from schema.station_status import StationSurgeSchema

app = APIRouter()


@app.get('/one/{station_code},{start_dt},{end_dt}', response_model=List[StationSurgeSchema],
         summary="获取单站的历史潮位集合")
def get_one_station_surges(station_code: str, start_dt: datetime, end_dt: datetime):
    surge_list = StationSurgeDao().get_station_surge_list(station_code, start_dt, end_dt)
    return surge_list
