package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 笛卡尔积
 */
public class Cartesian {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(Cartesian.class.getName()).setMaster("local[*]"));
        List<String> a = Arrays.asList("赵","钱", "孙", "李");
        List<Integer> b = Arrays.asList(1,2, 3, 4);
        JavaRDD<String> aRDD = jsc.parallelize(a);
        JavaRDD<Integer> bRDD = jsc.parallelize(b);
        aRDD.cartesian(bRDD).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+"----"+t._2);
            }
        });
    }
}
