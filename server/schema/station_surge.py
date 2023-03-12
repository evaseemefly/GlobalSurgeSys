from datetime import datetime
from pydantic import BaseModel, Field


class SurgeRealDataSchema(BaseModel):
    """
        潮位实况
        station_code: 站点编号
        surge: 潮位
        tid: 对应所属行政区划
    """
    station_code: str
    surge: str
    tid: int

