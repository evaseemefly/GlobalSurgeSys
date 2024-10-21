from typing import List

import arrow

from common.enums import ForecastAreaEnum
from conf.settings import DOWNLOAD_OPTIONS
from core.jobs import GlobalSurgeJob, IJob

REMOTE_ROOT_PATH: str = DOWNLOAD_OPTIONS.get('remote_root_path')
LOCAL_ROOT_PATH: str = DOWNLOAD_OPTIONS.get('local_root_path')


def daily_global_area_surge_forecast():
    """
        每日根据全部区域获取全球区域潮位预报产品定时任务

    :return:
    """
    temp_test_dt: arrow.Arrow = arrow.Arrow(2024, 10, 14, 00)

    issue_ts: int = temp_test_dt.int_timestamp
    """
    TODO:[*] 24-10-16  
    当前准备获取的发布时间戳
    """
    # areas: List[ForecastAreaEnum] = [ForecastAreaEnum.WNP, ForecastAreaEnum.AMERICA, ForecastAreaEnum.OCEANIA,
    #                                  ForecastAreaEnum.INDIA_OCEAN]

    areas: List[ForecastAreaEnum] = [ForecastAreaEnum.WNP, ForecastAreaEnum.INDIA_OCEAN]
    for area_temp in areas:
        surge_job_temp: IJob = GlobalSurgeJob(issue_ts, LOCAL_ROOT_PATH, area_temp, REMOTE_ROOT_PATH)
        surge_job_temp.to_do()


if __name__ == '__main__':
    daily_global_area_surge_forecast()
