from typing import List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request

# 本项目
from dao.region import RegionDao
from models.models import RegionInfo
from schema.region import RegionSchema, RegionFather

app = APIRouter()


@app.get('/list', response_model=List[RegionSchema], summary="获取指定的行政区划(默认Pid=-1)", )
def get_all_region(pid: int = -1, only_country: bool = True):
    """
        获取所属当前pid的全部region集合
    @param pid:
    @return:
    """
    region_list = []
    if only_country:
        region_list = RegionDao().get_all_region(pid)
    # elif only_country == False and pid != -1:
    #     region_list = RegionDao().get_all_region(pid)
    # pydantic.error_wrappers.ValidationError: 2 validation errors for RegionSchema
    return region_list


@app.get('/father/region', response_model=RegionSchema, summary="根据cid获取所属的父级 region")
def get_father_region(cid: int = -1):
    """
        根据 cid 获取所属的父 region
    :param cid:
    :return:
    """
    father = father_region = RegionDao().get_father_region(cid)
    return father


@app.get('/list/nest', response_model=List[RegionFather], summary="获取全部的行政区划(按照嵌套的方式)", )
def get_all_region_nest(pid: int = -1):
    """
        获取所属当前pid的全部region嵌套集合
    @param pid:
    @return:
    """
    region_nest_list = []
    list_region = RegionDao().get_region_nest_list(pid)
    return list_region
