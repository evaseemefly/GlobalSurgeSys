from scrapy.cmdline import execute


def main():
    """
        供pycharm调试使用的入口方法
    :return:
    """
    execute('scrapy crawl globalStationStatus'.split())
    pass


if __name__ == '__main__':
    main()
