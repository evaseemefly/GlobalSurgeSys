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
import os
import pathlib
import re

from typing import List

from core.db import DbFactory
from models.models import to_migrate
from models.models import StationAstronomicTideRealDataModel

engine = DbFactory().engine
session = DbFactory().Session


def print_hi(name):
    # 在下面的代码行中使用断点来调试脚本。
    print(f'Hi, {name}')  # 按 F9 切换断点。


def to_db():
    # 根据ORM生成数据库表
    to_migrate()
    pass


def to_do_astronomictide():
    dir_path: str = r'D:\01Proj\GlobalSurgeSys\background\byjupyter\base_data'
    start_dt_ar: arrow.Arrow = arrow.get(2023, 1, 1, 0, 0)
    list_pathes: List[pathlib.Path] = read_path_files_list(dir_path)
    for temp_path in list_pathes:
        # 从文件名中截取 station_code
        # eg: 1epme2_2023
        # 98caph_2023 | 94mona2_2023 | 101jrmi_2023
        # 截取 98caph 用正则匹配 字母到结尾
        temp_name_stamp: str = temp_path.name.split('_')[0]
        # 截取 station code
        re_str: str = '[A-Za-z]+[0-9]*$'
        re_code = re.findall(re_str, temp_name_stamp)
        temp_station_code: str = None
        if len(re_code):
            temp_station_code = re_code[0]
        # temp_station_code = temp_path.name.split('_')[0][1:]
        # print(temp_station_code)
        list_astronomic = read_all_astronomictide_2db(str(temp_path), temp_station_code, start_dt_ar)
        astronomictide_2db(list_astronomic, temp_station_code)
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
        # print('读取成功')
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


def astronomictide_2db(list_astronomi: List[dict], station_code: str):
    is_standard: bool = True
    for temp_ in list_astronomi:
        # TODO:[-] 23-04-07 注意此处需要加入判断
        if temp_['val'] == 999:
            is_standard = False
            continue
        else:
            if np.isnan(temp_['val']):
                is_standard = False
                continue
            else:
                temp_model = StationAstronomicTideRealDataModel(station_code=station_code,
                                                                gmt_realtime=temp_['dt'].datetime,
                                                                surge=temp_['val'],
                                                                ts=temp_['dt'].timestamp)
            session.add(temp_model)
    session.commit()
    if is_standard:
        # print(f'[-] 写入{station_code}成功~')
        pass
    else:
        print(f'[*] 写入{station_code}失败!')


def read_path_files_list(dir_path: str) -> List[pathlib.Path]:
    """
        读取 dir_path 目录下的全部文件并返回
    @param dir_path:
    @return:
    """
    scan_dir = os.scandir(dir_path)
    list_pathes: List[pathlib.Path] = []
    for temp_path in scan_dir:
        temp_pathlib: pathlib.Path = pathlib.Path(temp_path.path)
        list_pathes.append(temp_pathlib)
        # print(temp_pathlib)
    return list_pathes


def main():
    # to_db()
    to_do_astronomictide()


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    main()
# print_hi('PyCharm')

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
