package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 减少rdd分区的数量，该算子只能减少分区数量，如果要增加分区数量，需要使用repartition
 */
public class Coalesce {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(Coalesce.class.getName()).setMaster("local[*]"));
        List<String> lsit = Arrays.asList("nihao", "wohao", "dajiahao", "cccc", "ddddd","asd","234324","asd3");
        JavaRDD<String> rdds = jsc.parallelize(lsit,5);
        System.out.println(rdds.partitions().size());
        JavaRDD<String> newrdds = rdds.coalesce(2);
        System.out.println(newrdds.partitions().size());
    }
}
