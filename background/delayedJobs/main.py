import time
from typing import List

import arrow
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.util import utc
from memory_profiler import profile

from common import logger
from common.enums import ForecastAreaEnum
from common.logger import log_info, log_exception
from conf.settings import DOWNLOAD_OPTIONS
from core.jobs import GlobalSurgeJob, IJob

REMOTE_ROOT_PATH: str = DOWNLOAD_OPTIONS.get('remote_root_path')
LOCAL_ROOT_PATH: str = DOWNLOAD_OPTIONS.get('local_root_path')


# def get_nearly_forecast_time(now: arrow.Arrow) -> arrow.Arrow:
#     """
#     根据当前本地时间生成对应的预报产品时间（世界时 UTC）
#     * 根据当前utc时间生成对应的预报产品时间（世界时 UTC）
#     逻辑:
#         每日本地时间 07 时以后生成前一日 12 时（UTC）
#         每日本地时间 19 时生成当日 00 时（UTC）
#
#     @param now: 当前时间（arrow.Arrow 对象，假设为本地时间）
#     @return: 对应的预报产品时间戳（UTC）
#     """
#     # 将当前时间转换为 UTC
#
#     now_utc = now.to('UTC')
#     current_hour = now.hour
#
#     # [7,18] => 前一日 12H(UTC)
#     if current_hour < 7:
#         forecast_time = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
#     elif current_hour == 7:
#         forecast_time = now_utc.replace(hour=12, minute=0, second=0, microsecond=0)
#     elif 7 < current_hour < 19:
#         # 前一日 12H（UTC）
#         forecast_time = now_utc.shift(days=-1).replace(hour=12, minute=0, second=0, microsecond=0)
#     elif current_hour >= 19:
#         # 当日 00H（UTC）
#         forecast_time = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
#
#     return forecast_time


def get_nearly_forecast_time(now_utc: arrow.Arrow) -> arrow.Arrow:
    forecast_time_utc: arrow = now_utc
    current_hour = now_utc.hour

    # [7,18] => 前一日 12H(UTC)
    if current_hour <= 11:
        forecast_time = forecast_time_utc.shift(days=-1).replace(hour=12, minute=0, second=0, microsecond=0)
    elif 11 < current_hour <= 15:
        # 当日 00H（UTC）
        forecast_time = forecast_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    elif 16 <= current_hour <= 22:
        forecast_time = forecast_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    elif current_hour >= 22:
        forecast_time = forecast_time_utc.replace(hour=12, minute=0, second=0, microsecond=0)
    return forecast_time


@profile
def daily_global_area_surge_forecast():
    """
        每日根据全部区域获取全球区域潮位预报产品定时任务
        TODO:[-] 25-03-13 每次计划任务时根据当前时间获取对应的预报产品时间
        TODO:[*] 25-03-20: ERROR 出现内存溢出的bug
        [121356.831761] Out of memory: Killed process 18884 (python)
         total-vm:20012072kB, anon-rss:15336104kB, file-rss:0kB,
         shmem-rss:0kB, UID:0 pgtables:33888kB oom_score_adj:0
    :return:
    """
    # ubuntu18.04 是 utc 时间
    # TODO:[-] 25-03-17 需要将 时间 修改为 utc时间，获取now为
    # temp_now_dr: arrow.Arrow = arrow.now()
    temp_utc_now_dr: arrow.Arrow = arrow.utcnow()
    # temp_now_str: str = temp_now_dr.format('YYYYMMDD HH:mm:ss')
    temp_utc_now_str: str = temp_utc_now_dr.format('YYYYMMDD HH:mm:ss')

    forecast_issue_dr: arrow.Arrow = get_nearly_forecast_time(temp_utc_now_dr)
    forecast_issue_str: str = forecast_issue_dr.format('YYYYMMDD HH:mm:ss')
    forecast_issue_ts: int = forecast_issue_dr.int_timestamp
    """
    TODO:[*] 24-10-16  
    当前准备获取的发布时间戳
    """
    # areas: List[ForecastAreaEnum] = [ForecastAreaEnum.WNP]

    areas: List[ForecastAreaEnum] = [ForecastAreaEnum.WNP, ForecastAreaEnum.INDIA_OCEAN, ForecastAreaEnum.AMERICA,
                                     ForecastAreaEnum.OCEANIA, ]
    for area_temp in areas:
        log_info(
            f'当前时间:{temp_utc_now_str}|预报时间(utc):{forecast_issue_str}|预报区域:{area_temp.name}|执行处理操作...')
        try:
            surge_job_temp: IJob = GlobalSurgeJob(forecast_issue_ts, LOCAL_ROOT_PATH, area_temp, REMOTE_ROOT_PATH)
            surge_job_temp.to_do()
        except Exception as e:
            log_exception(
                f'当前时间:{temp_utc_now_str}|出现异常!|msg:{e.args}')


@profile
def test_job():
    log_info('执行耗时任务')
    a = [i for i in range(1000)]  # 占用大量内存
    time.sleep(1)  # 模拟耗时操作
    del a  # 释放内存
    time.sleep(1)
    log_info('结束耗时任务')


def test_delay_jobs():
    """
        根据时间测试任务执行情况
    @return:
    """
    # 定义起始时间和结束时间
    start = "2025-03-24T00:00:00"
    end = "2025-03-26T12:00:00"
    # 将字符串转换为 Arrow 对象
    start_time = arrow.get(start)
    end_time = arrow.get(end)

    # 生成时间数组，步长为 1 小时
    time_array = []
    current_time = start_time
    while current_time <= end_time:
        time_array.append(current_time)
        current_time = current_time.shift(hours=1)
    for temp_dt in time_array:
        temp_nearly_time = get_nearly_forecast_time(temp_dt)
        current_time_str: str = temp_dt.format("YYYY-MM-DD HH: mm:ss ZZ)")
        temp_nearly_time_str: str = temp_nearly_time.format("YYYY-MM-DD HH:mm:ss ZZ")
        msg = f'输入时间为:{current_time_str}, 对应临近时间为: {temp_nearly_time_str}'
        log_info(msg)


def main():
    # 尝试改为 blockingscheduler
    # scheduler = BlockingScheduler(timezone=utc)
    scheduler = BackgroundScheduler(timezone=utc)
    # 每日两次执行计划任务
    # locatime 19:00 utc 11:00
    # locatime 6:30  utc 22:30
    # TODO:[-] 25-03-24 修改为2次主动执行+2次补算
    scheduler.add_job(daily_global_area_surge_forecast, 'cron', hour=11, minute=0)
    scheduler.add_job(daily_global_area_surge_forecast, 'cron', hour=12, minute=0)
    scheduler.add_job(daily_global_area_surge_forecast, 'cron', hour=22, minute=30)
    scheduler.add_job(daily_global_area_surge_forecast, 'cron', hour=23, minute=30)
    # 每小时定时执行
    # scheduler.add_job(daily_global_area_surge_forecast, 'cron', minute=22)
    # scheduler.add_job(test_job, 'cron', minute=20)
    scheduler.start()
    # TODO:[-] 25-03-13 TEST: 根据当前时间执行下载操作
    # daily_global_area_surge_forecast()
    # TODO:[-] 25-03-24 TEST: 测试计划任务获取对应时间是否正确
    # test_delay_jobs()
    pass
    # TODO:[-] 25-03-19 加入了异常退出结束scheduler
    try:
        while True:
            # print(time.time())
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == '__main__':
    log_info('启动scheduler')
    main()
