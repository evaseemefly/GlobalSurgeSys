# -*- coding : UTF-8 -*-
import time
from schedule import every, repeat, run_pending
# from apscheduler.schedulers.twisted import TwistedScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess
import arrow
from scrapy.cmdline import execute
from datetime import datetime


# sched = BlockingScheduler()


# sys.setdefaultencoding('utf-8')


@repeat(every(1).minutes)
def delay_global_station():
    now_utc_str: str = arrow.utcnow().format('YYYYMMDDHHmm')
    print(f'doing delay task:{now_utc_str}')
    # 方法1:
    # subprocess.Popen('scrapy crawl globalStationStatus -s LOG_FILE=./logs/logs.log'.split()
    #                  )
    # 注意使用docker时，需要制定LOG_FILE 的绝对路径'
    # TODO:[*] 23-04-26
    #     raise child_exception_type(errno_num, err_msg, err_filename)
    # FileNotFoundError: [Errno 2] No such file or directory: 'scrapy': 'scrapy'
    # docker 作为解释器环境时，此种方式会出错
    # subprocess.Popen('scrapy crawl globalStationStatus -s LOG_FILE=/opt/project/logs/logs.log'.split(), shell=True
    #                  )
    # subprocess.Popen(
    #     'scrapy crawl globalStationStatus -s LOG_FILE=D:/01Proj/GlobalSurgeSys/background/spider/globalsurgespider/logs/logs.log'.split(),
    #     shell=True
    #     )
    # 在 docker 作为解释器环境时，使用此种方式
    execute('scrapy crawl globalStationStatus -s LOG_FILE=/opt/project/logs/logs.log'.split())
    # 方法2: 此种办法执行结束后会关闭
    # execute('scrapy crawl globalStationStatus'.split())


# @sched.scheduled_job('interval', seconds=60)
# def execute_global_station():
#     execute('scrapy crawl globalStationStatus'.split())


def main():
    """
        供pycharm调试使用的入口方法
    :return:
    """
    # execute('scrapy crawl globalStationStatus -s LOG_FILE=./logs/logs.log'.split())
    # TODO:[-] 23-03-01 此处修改为定时任务
    # TODO:[*] 23-04-26 对于docker部署时会使用 subprocess 会出现找不到文件的bug
    # 尝试使用 execute 直接执行的方式
    while True:
        run_pending()
        time.sleep(1)
    # 使用docker 可以直接使用此种方式调用
    # execute('scrapy crawl globalStationStatus -s LOG_FILE=./logs/logs.log'.split())
    # 方式2:直接结束
    # scheduler = TwistedScheduler()
    # # 每隔一小时执行一次
    # scheduler.add_job(execute_global_station, 'interval', seconds=60, next_run_time=datetime.now())
    # # 开始执行
    # scheduler.start()

    # 方式3:
    # ValueError: signal only works in main thread
    # sched.start()
    pass


if __name__ == '__main__':
    main()
