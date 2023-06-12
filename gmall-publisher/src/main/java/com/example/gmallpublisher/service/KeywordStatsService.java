package com.example.gmallpublisher.service;


import com.example.gmallpublisher.bean.KeywordStats;

import java.util.List;
/**
 * Desc: 关键词统计接口
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}