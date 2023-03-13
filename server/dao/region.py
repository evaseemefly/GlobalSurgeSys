from typing import List, Type, Any
from sqlalchemy import select, update
from sqlalchemy.orm import Session

# 本项目
from dao.base import BaseDao
from schema.region import RegionSchema, RegionFather
from models.models import RegionInfo


class RegionDao(BaseDao):

    def get_all_region(self, pid=-1):
        """
            获取指定pid的全部子区划及孙子行政区划
        @param pid:
        @return:
        """
        session: Session = self.db.session
        filter_query = session.query(RegionInfo).filter(RegionInfo.pid == pid).all()
        return filter_query

    def get_all_country(self):
        session: Session = self.db.session
        filter_query = session.query(RegionInfo).filter(RegionInfo.pid == -1).all()
        return filter_query

    def get_region_nest_list(self, pid: int = -1) -> List[Type[RegionFather]]:
        """
            以嵌套的方式获取 region 集合
        @param pid:
        @return:
        """
        session: Session = self.db.session

        # 父级 region
        father_region_query: List[Type[RegionInfo]] = session.query(RegionInfo).filter(RegionInfo.pid == pid).all()
        father_region_models: List[Type[RegionFather]] = []
        for temp_father in father_region_query:
            children: List[Type[RegionInfo]] = session.query(RegionInfo).filter(RegionInfo.pid == temp_father.id)
            temp_father_dict: dict[str, Any] = {
                'id': temp_father.id,
                'val_en': temp_father.val_en,
                'val_ch': temp_father.val_ch,
                'pid': temp_father.pid,
                'children': []
            }
            temp_father_model = RegionFather(**temp_father_dict)
            for temp_child in children:
                temp_child_dict = {
                    'id': temp_child.id,
                    'val_en': temp_child.val_en,
                    'val_ch': temp_child.val_ch,
                    'pid': temp_child.pid
                }
                temp_child_model = RegionSchema(
                    **temp_child_dict
                )
                temp_father_model.children.append(temp_child_model)
            father_region_models.append(temp_father_model)
        return father_region_models
