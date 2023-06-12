package com.example.gmallpublisher.service;


import com.example.gmallpublisher.bean.ProvinceStats;

import java.util.List;
/**
 * Desc: 地区维度统计接口
 */
public interface ProvinceStatsService {
    public List<ProvinceStats> getProvinceStats(int date);
}
