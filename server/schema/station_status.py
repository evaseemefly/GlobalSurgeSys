from datetime import datetime
from pydantic import BaseModel, Field


class StationStatusSchema(BaseModel):
    """
        海洋站状态 Schema
    """
    id: int
    station_code: str
    status: int
    gmt_realtime: datetime
    gmt_modify_time: datetime
    tid: int
