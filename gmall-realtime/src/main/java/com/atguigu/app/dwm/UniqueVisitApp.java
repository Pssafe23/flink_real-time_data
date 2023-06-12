package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;



//数据流： web/app->nginx->springboot->kafka(ods)->flinkapp->kafka(dwd)
//程序：  mocklog->nginx->logger.sh->kafka(ZK)->baselogapp->kafka
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取kafka dwd_page_log 主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        //3.将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);
        //4.过滤数据 状态编程 保留每个mid每天第一次登录的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.
                keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<>("date-state", String.class);
                //设置状态的超时时间以及更新时间的方式
                StateTtlConfig stateTtlConfig = new StateTtlConfig
                        .Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig
                                .UpdateType.OnCreateAndWrite).build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState=getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一条页面信息
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                //判断上一条页面是否为null
                if (lastPageId==null||lastPageId.length()<=0){
                    //取出状态数据
                    String lastDate = dateState.value();
                    //取出今天的日期
                    String curDate = simpleDateFormat.format(value.getLong("ts"));
                    //判断两个日期是否相同
                    if (!curDate.equals(lastDate)){
                        dateState.update(curDate);
                        return true;
                    }
//                    else return false;
                }
//                else {
                    return false;
//                }
            }
        });
        //5.将数据写入kafka
        uvDS.print();
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        //6.启动任务
        env.execute("UniqueVisitApp");

    }
}
