# flink_real-time_data

本项目是跟着尚硅谷的做的flink实时学习项目

# 流程

### 框架

![image](https://github.com/Pssafe23/flink_real-time_data/assets/88642338/f81fd465-58ef-4951-9a73-9f72aaffcf95)



### 指标

- 每个小时的点击次数和曝光次数
- 各广告平台的点击次数和曝光次数
- 各操作平台的点击次数和曝光次数
- 各省份城市的曝光点击数


### mysql 和 ods层表

## mysql

- 广告信息表
- 推广平台表
- 商品信息表
- 广告投放表
- 日志服务器列表

## ods层

- 广告信息表
- 推广平台表
- 商品信息表
- 广告投放表
- 日志服务器列表
- 广告检测日志表


### hive分层

- ods层
- dwd层和dim层

### clickhouse 

数据最终存放点

### flinebI

数据可视化


![image](https://github.com/Pssafe23/flink_real-time_data/assets/88642338/b175826e-0286-4fdc-a020-f99486a0167e)


# 问题

### 如何进行etl处理？

- 解析ip和ua
> 首先将日志中的各列进行解析为单独，并对ip和ua进行进一步的信息解析，得到ip对应的地理位置信息以及ua对应的浏览器和操作系统信息。

- 标注异常流量
> <font color="#ff0000">对已知爬虫ua列表进行判断</font>，如果是爬虫的ua，就对其进行封锁。如果不是爬虫ua列表，才对以下行为进行判断：<font color="#ff0000">同一ip访问过快，同一设备访问过快，同一ip固定周期访问，同一设备id周期进行访问</font>，对以上行为进行处理以及封锁



### 怎么进一步分析并提高效果

通过finebi可视化图，对指标的趋势进行分析，哪一种商品卖得更好，哪一个广告的营销更好都可以通过图更加清晰，并进行营销方针的改变，对哪一个广告的投放进行更改，达到改变营销效果。


### 数据怎么从hive传达到clickhouse

通过sparksql，将数据从hive---->clickhouse
