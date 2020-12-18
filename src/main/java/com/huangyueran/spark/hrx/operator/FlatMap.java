package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class FlatMap {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(FlatMap.class.getName()).setMaster("local[*]"));
        List<String> strings = Arrays.asList("ccc,aaa,vvv", "111,333,444", "r4,t5,y6");
        JavaRDD<String> parallelize = jsc.parallelize(strings);
        JavaRDD<String> objectJavaRDD = parallelize.flatMap(x -> Arrays.asList(x.split(",")).iterator());
        objectJavaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
