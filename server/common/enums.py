from enum import Enum, unique


class TaskTypeEnum(Enum):
    """
        任务状态枚举
    """
    HANGUP = 1004
    SUCCESS = 1001
    FAIL = 1002
    WAITING = 1003


class DataSourceEnum(Enum):
    """
        数据源枚举
    """
    GTS = 7002
    IOC = 7003
