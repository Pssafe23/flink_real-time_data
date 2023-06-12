package com.atguigu.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化 Phoenix 连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }
    //{"sinkTable":"dim_base_trademark","database":"gmall-210325-flink","before":{},"after":{"tm_name":"ddd","id":12},"type":"insert","tableName":"base_trademark"}
    //将数据写入 Phoenix：upsert into t(id,name,sex) values(...,...,...)
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement=null;
        try {
            //获取sql语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable, after);
            System.out.println(upsertSql);

            //预编译sql
            preparedStatement = connection.prepareStatement(upsertSql);

            //判断如果当前数据为更新操作，则先删除redis中的数据
            if ("update".equals(value.getString("type"))){
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(),after.getString("id"));
            }


            //执行插入操作
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement!=null){
                preparedStatement.close();
            }
        }
    }

    //"after":{"tm_name":"ddd","id":12},"type":"insert","tableName":"base_trademark"}
    //创建插入数据的 SQL upsert into t(id,name,sex) values('...','...','...')
    private String genUpsertSql(String sinkTable,JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." +
                sinkTable + "(" + StringUtils.join(keySet, ",") + ")" +
                " values('" + StringUtils.join(values, "','") + "')";
    }
}
