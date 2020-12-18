package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 对rdd数据进行过滤操作
 */
public class Filter {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(Filter.class.getName()).setMaster("local"));
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 3, 4, 5, 6, 7);
        JavaRDD<Integer> parallelize = jsc.parallelize(integers);
        JavaRDD<Integer> filter = parallelize.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                if (v1 % 2 == 0) {
                    return true;
                }
                return false;
            }
        });
        filter.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
