package com.example.gmallpublisher.service;


import com.example.gmallpublisher.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;
import java.util.List;
/**
 * Desc: 访客流量统计 Mapper
 */
public interface VisitorStatsService {
    public List<VisitorStats> getVisitorStatsByNewFlag(int date);
    public List<VisitorStats> getVisitorStatsByHour(int date);
    public Long getPv(int date);
    public Long getUv(int date);

}

