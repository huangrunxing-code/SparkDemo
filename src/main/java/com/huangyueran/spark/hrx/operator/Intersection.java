package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 取两个rdd的交集
 */
public class Intersection {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(Intersection.class.getName()).setMaster("local[*]"));
        List<String> a = Arrays.asList("aaa", "bbb", "ccc", "ddd");
        List<String> b = Arrays.asList("aaa1", "bbb", "ccc1", "ddd");
        JavaRDD<String> ardd = jsc.parallelize(a);
        JavaRDD<String> brdd = jsc.parallelize(b);
        JavaRDD<String> intersection = ardd.intersection(brdd);
        intersection.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
