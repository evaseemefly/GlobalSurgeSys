from datetime import datetime

from pydantic import BaseModel


class CoverageFileInfoSchema(BaseModel):
    """
        栅格 file 基础信息
    """
    forecast_time: datetime
    forecast_ts: int
    release_time: datetime
    release_ts: int
    area: int
    relative_path: str
    file_name: str

    class Config:
        orm_mode = True
