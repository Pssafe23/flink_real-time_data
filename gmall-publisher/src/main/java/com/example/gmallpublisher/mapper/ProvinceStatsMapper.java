package com.example.gmallpublisher.mapper;


import com.example.gmallpublisher.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;
import java.util.List;
/**
 * Desc: 地区维度统计 Mapper
 */
public interface ProvinceStatsMapper {
    //按地区查询交易额
    @Select("select province_name,sum(order_amount) order_amount " +
            "from province_stats_210325 where toYYYYMMDD(stt)=#{date} " +
            "group by province_id ,province_name ")
    public List<ProvinceStats> selectProvinceStats(int date);
}

