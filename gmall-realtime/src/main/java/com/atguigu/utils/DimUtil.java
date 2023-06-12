package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection,String tableName,String id)throws Exception{

        //查询phoenix之前先查询redis
        Jedis jedis = RedisUtil.getJedis();
        //DIM:DIM_USER_INFO:104
        String redisKey = "DIM" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr!=null){

            //重置过期时间
            jedis.expire(redisKey,24*60*60);
            //归还连接
            jedis.close();
            //返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        //根据查询语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";
        //查询phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);
        //在返回结果之前，将数据写入redis
        jedis.set(redisKey,dimInfoJson.toJSONString());
        jedis.expire(redisKey,24*60*60);
        jedis.close();
        //返回结果
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName,String id){
        Jedis jedis = RedisUtil.getJedis();
        String redisKey="DIM"+tableName+":"+id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception{
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,"DIM_BASE_TRADEMARK","19"));
        long end = System.currentTimeMillis();
//        System.out.println(getDimInfo(connection,"DIM_USER_INFO","19"));
//        long end2 = System.currentTimeMillis();
//        System.out.println(getDimInfo(connection,"DIM_USER_INFO","19"));
//        long end3 = System.currentTimeMillis();
        System.out.println(end-start);
//        System.out.println(end2-end);
//        System.out.println(end3-end2);
        connection.close();

    }
}
