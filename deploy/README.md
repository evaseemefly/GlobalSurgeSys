1. ### 部署文档  

本系统共分为:  
server 服务端
部署目录为`/home/nmefc/proj`

后台服务目录结构

```
global_surge_server/
├── server
└── spider
```

发布的服务:

1. mysql 服务，通过 volumes 将数据库原文件挂在至宿主机

2. fastapi 服务, 基于打包后的当前环境的 fastapi 服务容器环境

3. spider爬虫服务,依赖 fastapi 的 python 容器环境

   

依赖的外部服务:

1. geoserver 服务



目录结构

