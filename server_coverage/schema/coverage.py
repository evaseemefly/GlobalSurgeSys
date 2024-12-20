from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, Field


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


class CoverageVectorSchema(BaseModel):
    """
        栅格数据提取的(矢量)预报数据集
    """
    timestamp_list: List[int]
    """预报时间戳集合"""
    vals: List[Optional[float]] = Field(None)
    """对应的预报值集合"""

    class Config:
        orm_mode = False
