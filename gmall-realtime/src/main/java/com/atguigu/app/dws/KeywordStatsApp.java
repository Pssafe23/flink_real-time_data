package com.atguigu.app.dws;

import com.atguigu.app.function.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.utils.ClickhouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.使用DDL方式读取kafka数据创建表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        tableEnv.executeSql("create table page_view( " +
                "    `common` Map<STRING,STRING>, " +
                "    `page` Map<STRING,STRING>, " +
                "    `ts` BIGINT, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                ") with (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");

        // 3.过滤数据 上一跳页面为"search" and 搜索词 is not null
        Table fullWordTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] full_word, " +
                "    rt " +
                "from  " +
                "    page_view " +
                "where " +
                "    page['last_page_id']='search' and page['item'] is not null");

        // 4.注册函数UDF,进行分词处理
        tableEnv.createTemporarySystemFunction("split_words", SplitFunction.class);
        Table wordTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word,  " +
                "    rt " +
                "FROM  " +
                "    " + fullWordTable + ", LATERAL TABLE(split_words(full_word))");

        // 5.分组 ，开窗，聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from " + wordTable + " " +
                "group by " +
                "    word, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        // 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        // 7.将数据打印并写入clickHouse
        keywordStatsDataStream.print();
        keywordStatsDataStream.addSink(ClickhouseUtil.getSink("insert into keyword_stats_210325(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));
        // 8.启动任务
        env.execute("KeywordStatsApp");
    }
}
