import pathlib
import os
from typing import Optional

import pandas as pd
import numpy as np
import pathlib
import xarray as xr
import rioxarray

from common.enums import ElementTypeEnum, RasterFileType
from common.exceptions import FileDontExists, FileReadError, FileTransformError
from mid_models.files import IForecastProductFile, ForecastSurgeRasterFile


class GlobalSurgeTransformer:
    """
        全球预报产品转换器
    """

    def __init__(self, file: IForecastProductFile):
        self.file = file
        self._ds: xr.Dataset = None
        """标准化后的 h dataset """

    def read_data(self, var_name: str = "h"):
        """
            根据 self.file 读取文件并将ds 写入 self._ds
            若异常则不写入 ds
        :param var_name: variables 名称
        :return:
        """
        if self.file.exists():
            try:
                # step1-1: 读取指定文件
                data: xr.Dataset = xr.open_dataset(self.file.local_full_path)
                # step1-2: 读取陆地掩码
                data[var_name] = data[var_name].where(data['maskLand'] == 0, np.nan)
                # TODO:[-] 24-11-06 此处加入了根据阈值进行过滤的filter mask步骤
                data_mask = (data[var_name] <= -0.3) | (data[var_name] >= 0.3)
                # TODO:[-] 24-11-06 此部分不需要删除被掩码掉的部分，只填充nan
                filtered_ds_h = data.where(data_mask, drop=False)
                # data['hMax'] = data['hMax'].where(data['maskLand' == 0, np.nan])
                # step1-3: 对于维度进行倒叙排列
                data_standard: xr.Dataset = filtered_ds_h.sortby('latitude', ascending=False)
                first_time = data_standard["Time"].values[0]
                # step1-4: 设置空间坐标系与分辨率
                first_ds = data_standard.sel(Time=first_time)[var_name]
                # 设置空间坐标系与分辨率
                first_ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)
                first_ds.rio.write_crs("EPSG:4326", inplace=True)
                self._ds = first_ds
            except Exception as e:
                # TODO:[*] 24-09-25 需加入logger
                # TODO:[*] 25-03-18 (-101, 'NetCDF: HDF error')
                # TODO:[*] 26-01-15 缺少 netcdf 库导致的错误 —— conda install netCDF4
                # found the following matches with the input file in xarray's IO backends: ['netcdf4', 'h5netcdf']. But their dependencies may not be installed, see:
                # https://docs.xarray.dev/en/stable/user-guide/io.html
                # https://docs.xarray.dev/en/stable/getting-started-guide/installing.html
                raise FileReadError
            pass
        else:
            raise FileDontExists()

    def out_put(self, compress="deflate", diver: str = 'GTiff') -> Optional[
        ForecastSurgeRasterFile]:
        """
            输出并转换为 geotiff
        :param compress: 压缩方法
        :param diver:   输出文件类型
        :return:
        """
        raster_file: Optional[ForecastSurgeRasterFile] = None
        """输出的栅格文件"""
        element_type: ElementTypeEnum = ElementTypeEnum.SURGE
        raster_type: RasterFileType = RasterFileType.GEOTIFF
        if self._ds is not None:
            try:
                # 将 'field_2024-09-22_18_00_00.f0.nc' -> 'field_2024-09-22_18_00_00.f0.tif'
                # ['field_2024-10-14_00_00_00', 'f0']
                file_splits = self.file.file_name.split('.')[:2]
                # TODO:[-] 24-11-07 此处加入输出文件名称若包含 hmax 则 由 'field_2024-09-22_18_00_00.f0.tif' -> 'field_2024-09-22_18_00_00.f0.hmax.tif'
                if self.file.is_hmax_file():
                    file_splits.append('hmax')
                file_splits.append('tif')
                transformer_file_name: str = '.'.join(file_splits)
                out_put_file_path: str = str(pathlib.Path(
                    self.file.local_root_path) / self.file.relative_path / transformer_file_name)
                self._ds.rio.to_raster(out_put_file_path, diver=diver, compress=compress)
                raster_file = ForecastSurgeRasterFile(self.file.area, element_type,
                                                      raster_type, transformer_file_name,
                                                      self.file.relative_path,
                                                      self.file.local_root_path)
            except Exception as e:
                raise FileTransformError()
        return raster_file
