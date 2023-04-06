# 这是一个示例 Python 脚本。
import arrow
# 按 Ctrl+F5 执行或将其替换为您的代码。
# 按 按两次 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import numpy as np
import netCDF4 as nc
import pandas as pd
import numpy.ma as ma
import matplotlib as mpl
import matplotlib.pyplot as pltz
import xarray as xar
import rioxarray
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Mapped
from sqlalchemy import ForeignKey, Sequence, MetaData
from sqlalchemy import select, update
from sqlalchemy.orm import mapped_column, DeclarativeBase
from datetime import datetime

from typing import List

from models.models import to_migrate
from models.models import StationAstronomicTideRealDataModel


def print_hi(name):
    # 在下面的代码行中使用断点来调试脚本。
    print(f'Hi, {name}')  # 按 F9 切换断点。


def to_db():
    # 根据ORM生成数据库表
    to_migrate()
    pass


def read_all_astronomictide_2db(full_path: str, station_code: str, start_dt_ar: arrow.Arrow) -> List[dict]:
    """
        读取指定路径下的指定天文潮文件并写入db
    @param full_path: 读取的目标路径全名称
    @param station_code: 站点code
    @param start_dt_ar: 读取天文潮的起始时间(指定年份的1-1 00:00)
    @return:
    """
    with open(full_path, 'rb') as f:
        data = pd.read_table(f, sep='\s+', encoding='unicode_escape', header=None, infer_datetime_format=False)
        print('读取成功')
        index_days = 0
        list_days = []
        list_realdata = []
        for day in range(data.shape[0]):
            current_day_ar: arrow.Arrow = start_dt_ar.shift(days=day)
            list_days.append(current_day_ar)
            for hour in range(data.shape[1]):
                current_day_dt_ar: arrow.Arrow = current_day_ar.shift(hours=hour)
                temp_val = data.iloc[day, hour]
                temp_ = {'dt': current_day_dt_ar, 'val': temp_val}
                list_realdata.append(temp_)
        return list_realdata


def main():
    to_db()


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    main()
# print_hi('PyCharm')

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
