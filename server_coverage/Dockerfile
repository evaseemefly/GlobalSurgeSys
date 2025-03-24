FROM py37:1.27

ENV PYTHONUNBUFFERED 1

# 存储于 /home/surge/DATA_STORE
#MKDIR /data/local_wind_nwp

# 2- 将 /opt/project 设置为工作目录
WORKDIR /opt/project

# 3- 将本地目录下的文件全部拷贝至容器 /opt/project 中
COPY /home/surge/pro/global_surge_forecast_coverageserver /opt/project