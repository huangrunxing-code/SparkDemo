package com.huangyueran.spark.hrx.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import scala.Function1;

//每十秒钟统计过去60秒 搜索前3的词     hrx  nn
public class WindowHotWord {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("WindowHotWord")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> lines = ss.readStream()
                .format("scoket")
                .option("host", "rhel071")
                .option("port", 9999)
                .load();
      /*  Dataset<String> map = lines.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return String.valueOf(row.get(1));
            }
        });*/

    }
}
