from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, Field


class GTSPointSchema(BaseModel):
    """
        GTS 点位观测数据
    """
    dt_utc: datetime
    timestamp_utc: int
    sea_level_meters: float


class GTSEntiretySchema(BaseModel):
    """
        GTS 指定站点的观测数据集合
    """
    station_code: str
    sensor_type: str
    source_file: str
    data_points: List[GTSPointSchema]


# class GTSListSchema(BaseModel):


class StationsSurgeSchema(BaseModel):
    """
        站点 surge schema
    """
    dt: datetime
    surge: float
    ts: int


class StationSurgeListSchema(BaseModel):
    """
        海洋站潮位集合 schema list
    """
    station_code: str
    tid: int
    surge_list: List[StationsSurgeSchema]

    class Config:
        orm_mode = False
