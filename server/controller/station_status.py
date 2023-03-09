from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

from dao.station_status import StationStatusDao
from models.models import StationStatus
from schema.station_status import StationStatusSchema

app = APIRouter()


@app.get('/station/status/all', response_model=StationStatusSchema)
def get_all_station_status():
    station_res = StationStatusDao.get_all_station_status()
    return station_res
