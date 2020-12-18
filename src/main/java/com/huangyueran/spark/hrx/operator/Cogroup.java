package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * cogroup  把 2-4个rdd 按照相同key进行组合
 */
public class Cogroup {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(Cogroup.class.getName()).setMaster("local[*]"));
        List<Tuple2<Integer,String>> l1=new ArrayList<>();
        l1.add(new Tuple2<>(1,"第一个"));
        l1.add(new Tuple2<>(2,"第二个"));
        l1.add(new Tuple2<>(3,"第三个"));
        l1.add(new Tuple2<>(4,"第四个"));
        List<Tuple2<Integer,Integer>> l2=new ArrayList<>();
        l2.add(new Tuple2<>(1,1));
        l2.add(new Tuple2<>(2,2));
        l2.add(new Tuple2<>(3,3));
        l2.add(new Tuple2<>(4,4));
        List<Tuple2<Integer,Boolean>> l3=new ArrayList<>();
        l3.add(new Tuple2<>(1,false));
        l3.add(new Tuple2<>(2,false));
        l3.add(new Tuple2<>(3,false));
        l3.add(new Tuple2<>(4,false));
        List<Tuple2<Integer,Double>> l4=new ArrayList<>();
        l4.add(new Tuple2<>(1,4.23));
        l4.add(new Tuple2<>(2,3.65));
        l4.add(new Tuple2<>(3,6.52));
        l4.add(new Tuple2<>(4,4.29));
        List<Tuple2<Integer,String>> l5=new ArrayList<>();
        l5.add(new Tuple2<>(1,"ss"));
        l5.add(new Tuple2<>(2,"vvvv"));
        l5.add(new Tuple2<>(3,"erf"));
        l5.add(new Tuple2<>(4,"asdsa"));

        JavaPairRDD<Integer, String> r1 = jsc.parallelizePairs(l1);
        JavaPairRDD<Integer, Integer> r2 = jsc.parallelizePairs(l2);
        JavaPairRDD<Integer, Boolean> r3 = jsc.parallelizePairs(l3);
        JavaPairRDD<Integer, Double> r4 = jsc.parallelizePairs(l4);
        JavaPairRDD<Integer, String> r5 = jsc.parallelizePairs(l5);//最多只能4个

        JavaPairRDD<Integer, Tuple4<Iterable<String>, Iterable<Integer>, Iterable<Boolean>, Iterable<Double>>> cogroup = r1.cogroup(r2, r3, r4);
        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple4<Iterable<String>, Iterable<Integer>, Iterable<Boolean>, Iterable<Double>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple4<Iterable<String>, Iterable<Integer>, Iterable<Boolean>, Iterable<Double>>> t) throws Exception {
                System.out.println(t._1 + "==" + t._2);
            }
        });
    }
}
