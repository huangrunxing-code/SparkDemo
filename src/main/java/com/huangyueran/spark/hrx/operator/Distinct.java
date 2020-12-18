package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/*
    对rdd进行去重操作
 */
public class Distinct {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(Distinct.class.getName()).setMaster("local[*]"));
        List<String> strings = Arrays.asList("ni", "ni", "wo", "ta");
        jsc.parallelize(strings).distinct().foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
