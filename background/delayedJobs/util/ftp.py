import uuid
from typing import List

import arrow
import ftplib
# TODO:[-] 24-10-14 注意此处使用sftp，需要使用 pysftp库
import pysftp
import os
import pathlib

from loguru import logger


class FtpClient:
    """
        + 23-09-20 ftp下载工厂类
    """

    # encoding = 'gbk'
    # TODO:[-] 24-02-26 由于目录存在中文设置为 utf-8
    encoding = 'utf-8'
    ftp = ftplib.FTP()

    def __init__(self, host: str, port=21):
        self.ftp.connect(host, port)
        self.ftp.encoding = self.encoding
        # TODO:[*] 24-03-27 此处修改
        # self.ftp = ftplib.FTP(host=host, encoding=self.encoding)

    def login(self, user, passwd):
        self.ftp.login(user, passwd)
        print(self.ftp.welcome)

    def down_load_file(self, local_full_path: str, remote_file_name: str):
        """


        @param local_full_path: 本地存储全路径(含文件名),
        @param remote_file_name: 远端文件名
        @return:
        """
        file_handler = open(local_full_path, 'wb')
        # print(file_handler)
        # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
        self.ftp.retrbinary('RETR ' + remote_file_name, file_handler.write)
        file_handler.close()
        return True

    def down_load_file_bycwd(self, local_full_path: str, remote_path: str, file_name: str) -> bool:
        """
            - 24-02-27 此处需要修改 remote_path 为远端的绝对路径，由于下载不同的文件需要反复切换至不同的目录，使用相对路径较为不便
            改为使用绝对路径
        @param local_full_path:
        @param remote_path:
        @param file_name:
        @return:
        """
        is_ok: bool = False
        file_handler = open(local_full_path, 'wb')
        # print(file_handler)
        # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
        # 进入到 remote_path 路径下
        # ftplib.error_perm: 550 Failed to change directory.
        # remote_path:              '/test/ObsData/SHW/2024/02/20'
        # 实际路径: /home/nmefc/share/test/ObsData/汕尾/perclock/2024/02
        # v2:                     '/test/ObsData/汕尾/perclock/2024/02/20'
        # v3:     '/home/nmefc/share/test/ObsData/汕尾/perclock/2024/02/20'
        # -- 修改为实际的绝对路径
        # TODO:[*] 24-03-27
        # 路径错误
        # ftplib.error_perm: 550 The system cannot find the path specified.
        # 改为 linux 系统路径格式后仍然出错
        # ftplib.error_temp: 451 No mapping for the Unicode character exists in the target multi-byte code page.
        # remote_path = r'/home/nmefc/share/test/ObsData'
        # TODO:[*] 24-07-16 测试错误 ftplib.error_perm: 550 Failed to change directory.
        self.ftp.cwd(remote_path)
        files_list: List[str] = self.ftp.nlst()
        # TODO:[-] 23-09-26 修改 ftp 缓冲区为 250KB
        BUFSIZE: int = 262144
        if file_name in files_list:
            now_str: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            print(f'')
            logger.info(
                f'[-]下载指定文件:{file_name}ing')
            self.ftp.retrbinary('RETR ' + file_name, file_handler.write, BUFSIZE)
            is_ok = True
            load_over_time: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            logger.info(
                f'[-]下载指定文件:{file_name.lower()}成功')
            file_handler.close()
        # TODO:[*] 24-02-27 此处缺少对于小写的处理
        elif file_name.lower() in files_list:
            now_str: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            # print(f'')
            # print(f'{now_str}[*]下载指定文件:{file_name}ing')
            logger.info(
                f'[-]下载指定文件:{file_name}ing')
            self.ftp.retrbinary('RETR ' + file_name.lower(), file_handler.write, BUFSIZE)
            is_ok = True
            load_over_time: str = arrow.Arrow.utcnow().format('YYYY-MM-DD|HH:mm:ss')
            logger.info(
                f'[-]下载指定文件:{file_name.lower()}成功')
            file_handler.close()
        return is_ok

    def down_load_file_tree(self, local_path: str, remote_path: str, cover_path: str = None):
        """
            下载整个目录下的文件
        @param local_path:
        @param remote_path:
        @param cover_path:
        @return:
        """
        print("远程文件夹remoteDir:", remote_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        # 进入到 remote_path 路径下
        self.ftp.cwd(remote_path)
        remote_name_list = self.ftp.nlst()
        print("远程文件目录：", remote_name_list)
        # ['EU_high_res_wind_2023082012.nc']
        for file in remote_name_list:
            # 'E:\\05DATA\\06wind\\2022_EU_high'
            Local = os.path.join(local_path, file)
            print("正在下载", self.ftp.nlst(file))
            if file.find(".") == -1:
                if not os.path.exists(Local):
                    os.makedirs(Local)
                self.down_load_file_tree(Local, file)
            else:
                self.down_load_file(Local, file)
        self.ftp.cwd("..")
        return

    def get_nlist(self, remote_path: str) -> List[str]:
        """
            获取指定路径下的所有文件
        :param remote_path:
        :return:
        """
        self.ftp.cwd(remote_path)
        return self.ftp.nlst()

    def close(self):
        self.ftp.quit()


class SFTPClient:
    """
        SFTP实现类
    """

    my_cnopts = pysftp.CnOpts()
    my_cnopts.hostkeys = None  # 禁用主机密钥验证

    def __init__(self, hostname, username, password, port=22):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                cnopts=self.my_cnopts
            )
            print("Connection successfully established.")
        except Exception as e:
            # TODO:[*] 24-10-16 无法连接
            # ERROR: Failed to connect: No hostkey for host 128.5.6.16 found.
            """
                    raise SSHException("No hostkey for host %s found." % host)
                    paramiko.ssh_exception.SSHException: No hostkey for host 128.5.6.16 found.
            """
            print(f"Failed to connect: {e}")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Connection closed.")

    def download_file(self, local_path: str, remote_path: str, file_name: str):
        remote_full_path: str = str(pathlib.PurePosixPath(remote_path) / file_name)
        local_full_path: str = str(pathlib.Path(local_path) / file_name)
        try:
            # 判断本地是否存在指定目录，不存在咋创建
            if pathlib.Path(local_path).is_dir() is False:
                pathlib.Path(local_path).mkdir(parents=True)
            # TODO:[*] 24-10-16 下载错误
            # '/mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024101400/nc_latlon/WNP/
            # field_2024-10-14_00_00_00.f0.nc/field_2024-10-14_00_00_00.f0.nc'
            self.connection.get(remote_full_path, local_full_path)
            print(f"Downloaded: {remote_full_path} to {local_full_path}")
        except Exception as e:
            # Failed to download file: [Errno 2] No such file or directory: 'E:\\05DATA\\01nginx_data\\nmefc_download\\WD_RESULT\\WNP\\model_output\\2024101400\\nc_latlon\\WNP\\field_2024-10-14_00_00_00.f0.nc'
            print(f"Failed to download file: {e}")
            pass

    def download_files_in_cwd(self, local_dir: str, remote_path: str):
        """
            根据远端 remote_path 批量下载文件
        @param local_dir:
        @param remote_path:
        @return:
        """
        try:
            # 切换到远程工作目录
            self.connection.chdir(remote_path)
            print(f"Changed directory to: {remote_path}")

            # 确保本地目录存在
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)

            # 列出远程目录中的文件并下载
            files = self.connection.listdir()
            for file in files:
                self.download_file(remote_path, local_dir, file)
        except Exception as e:
            print(f"Failed to download files: {e}")

    def get_nlist(self, remote_path: str) -> List[str]:
        # 列出远程目录中的文件并下载
        # TODO:[*] 24-10-16 路径有误，目前路径已重新修改
        # ERROR     raise IOError(errno.ENOENT, text)
        # FileNotFoundError: [Errno 2] No such file
        #     '/mnt/home/nmefc/surge/surge_glb_data/IndiaOcean/WNP/model_output/2024101400/nc_latlon/WNP'
        # 实际 /mnt/home/nmefc/surge/surge_glb_data/IndiaOcean/model_output/2024101400/nc_latlon/IndiaOcean
        #     '/mnt/home/nmefc/surge/surge_glb_data/WNP/model_output/2024101400/nc_latlon/WNP'
        # 切换目录为 remote_path
        self.connection.cwd(remote_path)
        files: List[str] = self.connection.listdir()
        """获取当前目录下的所有文件集合"""
        return files
