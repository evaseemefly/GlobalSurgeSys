from typing import List, Optional, Any

from models.models import StationStatus, RegionInfo, StationInfo
from schema.station_status import StationStatusAndGeoInfoSchema
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from dao.base import BaseDao


class StationDao(BaseDao):
    @classmethod
    def get_station_by_code(cls, list_station: List[StationInfo], code: str) -> Optional[StationInfo]:
        """
            根据站点代码获取站点信息
        :param list_station:
        :param code:
        :return:
        """
        for station in list_station:
            if station.station_code == code:
                return station
        return None

    def get_all_station(self) -> List[StationInfo]:
        """
            获取所有站点信息
        :return:
        """
        session: Session = self.db.session
        return session.query(StationInfo).filter(StationInfo.is_del == 0).all()