from common.default import MS


def timestamp_sec2ms(val: int) -> int:
    """
        时间戳转换: s -> ms
    @param val:
    @return:
    """
    return val * MS


def timestamp_ms2sec(val: int) -> int:
    """
        时间戳转换: ms -> s
    @param val:
    @return:
    """
    return val / MS
