import schedule
import time
from schedule import every, repeat, run_pending
import subprocess
from datetime import datetime
import arrow
from scrapy.cmdline import execute


@repeat(every(30).minutes)
def delay_global_station():
    now_utc_str: str = arrow.utcnow().format('YYYYMMDDHHmm')
    print(f'测试定时任务:{now_utc_str}')
    # 方法1:
    subprocess.Popen('scrapy crawl globalStationStatus')
    # 方法2: 此种办法执行结束后会关闭
    # execute('scrapy crawl globalStationStatus'.split())


def execute_global_station():
    execute('scrapy crawl globalStationStatus'.split())



def main():
    """
        供pycharm调试使用的入口方法
    :return:
    """
    execute('scrapy crawl globalStationStatus -s LOG_FILE=./logs/logs.log'.split())
    # TODO:[-] 23-03-01 此处修改为定时任务
    # while True:
    #     run_pending()
    #     time.sleep(1)
    pass


if __name__ == '__main__':
    main()
