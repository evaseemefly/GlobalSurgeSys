from datetime import datetime
from arrow import Arrow
import arrow

from common.enums import DataSourceEnum
# 配置文件
from config.tb_config import DB_TABLE_SPLIT_OPTIONS


def get_tablename_by_source(dt: datetime, source: DataSourceEnum) -> str:
    """
        TODO: 25-12-05 新增：根据数据源和时间动态获取表名
        根据数据源类型生成表名
        IOC: station_realdata_specific_ioc_YYYYMM
        GTS: station_realdata_specific_YYYYMM (假设 GTS 为默认或根据实际情况修改)
    """
    tab_base_name: str = DB_TABLE_SPLIT_OPTIONS.get('station').get('tab_split_name')

    base_name = tab_base_name
    # 如果是 IOC 数据源，增加后缀
    if source == DataSourceEnum.IOC:
        base_name = f"{tab_base_name}_ioc"

    # TODO: 25-12-05 修改为使用 arrow 格式化时间
    dt_str = arrow.get(dt).format('YYYYMM')
    return f"{base_name}_{dt_str}"


def get_split_tablename(dt: datetime, source: DataSourceEnum = DataSourceEnum.GTS) -> str:
    """
        根据时间获取对应的分表
        + 25-12-05 加入了根据数据源获取对应表的判断
    @param dt:
    @return:
    """
    # tab_base_name: str = DB_TABLE_SPLIT_OPTIONS.get('station').get('tab_split_name')
    # tab_dt_name: str = arrow.get(dt).format('YYYYMM')
    # tab_name: str = f'{tab_base_name}_{tab_dt_name}'
    tab_name = get_tablename_by_source(dt, source)
    return tab_name
