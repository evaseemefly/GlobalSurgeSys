"""
    + 24-01-19 自定义异常
"""


class FileDontExists(Exception):
    """
        文件不存在 异常
    """
    pass


class FtpDownLoadError(Exception):
    """
        fpt 下载异常
    """
    pass


class FileReadError(Exception):
    """
        文件读取错误
    """
    pass


class FileTransformError(Exception):
    """
        文件转换错误
    """
    pass


class ReadataStoreError(Exception):
    """
        实况写入数据库异常
    """
    pass

class CoverageStoreError(Exception):
    """
        栅格图层写入db异常
    """
    pass


class FileFormatError(Exception):
    """
        文件格式错误
    """
    pass
