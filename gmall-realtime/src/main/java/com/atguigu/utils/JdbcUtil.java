package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection,String querySql,Class<T> clz,boolean underScoreToCamel)
            throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        //创建集合用于存放查询结果数据
        ArrayList<T> resultlist = new ArrayList<>();
        //预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()){
            //创建泛型对象
            T t = clz.newInstance();
            //给泛型对象赋值
            //sql中是从1开始
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转换为驼峰命名
                if (underScoreToCamel){
                    columnName= CaseFormat.LOWER_UNDERSCORE
                    .to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }
                //获取列值
                Object value = resultSet.getObject(i);
                //给泛型对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            //将该对象添加至集合
            resultlist.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return resultlist;
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,"aa_bb"));
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        List<JSONObject> queryList = queryList(connection,
                "select * from GMALL210325_REALTIME.DIM_USER_INFO",
                JSONObject.class, false);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }
        connection.close();
    }

}
