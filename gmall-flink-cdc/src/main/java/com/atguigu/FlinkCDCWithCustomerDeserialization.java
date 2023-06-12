package com.atguigu;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.通过FlinkCDC构筑SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("node1")
                .port(3306)
                .username("root")
                .password("hadoop")
                .databaseList("gmall-210325-flink") //连接数据库
                .tableList("gmall-210325-flink.base_trademark") //如果不添加该参数，则消耗指定数据库中所有表的数据，如果指定，指定方式db.table
                .deserializer(new CustomerDeserialization())
                //StartupOptions.initial()做了一次快照，全量数据加载,每次都是初始化，所以可以打印
                //StartupOptions.latest() 会从数据库搜索binlog信息，相当于做了快照，不会在打印
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        //3.打印任务
        streamSource.print();
        //4.启动任务
        env.execute("FlinkCDCWithCustomerDeserialization");
    }
}
