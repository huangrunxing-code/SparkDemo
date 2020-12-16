package com.huangyueran.spark.hrx.datacenter;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

public class mainclear {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("mainclear").master("local[*]")
                .config("hadoop.home.dir", "/user/hive/warehouse")
                //.config("spark.driver.memory",471859210)
                .enableHiveSupport()
                .getOrCreate();
       // ss.sql("select * from text.exter_jushuju_middle limit 10").show();
        ss.sql("show databases").show();

        //加载配置文件

        String split_str;//分隔符

        String source_db;//元数据库名
        String source_table;//元数据库表
        String source_schema;//元数据表结构



        String target_db;//基础数据库名
        String target_table;//基础数据库表
        String target_schme;//基础数据表结构


        String table_field;//字段列表    field1,field2,field3,field4


        /**
         *
         *
         *
         *
         */
    }

}
