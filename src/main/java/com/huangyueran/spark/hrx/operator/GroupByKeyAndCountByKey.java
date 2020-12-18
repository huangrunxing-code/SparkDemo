package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * groupby根据传入的方法去分组
 * countbykey
 */
public class GroupByKeyAndCountByKey {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(GroupByKeyAndCountByKey.class.getName()).setMaster("local[*]"));
        List<Tuple2<Integer,String>> cc =new ArrayList<>();
        cc.add(new Tuple2<>(1,"aa"));
        cc.add(new Tuple2<>(2,"bb"));
        cc.add(new Tuple2<>(5,"cc"));
        cc.add(new Tuple2<>(3,"dd"));
        cc.add(new Tuple2<>(2,"ff"));
        JavaRDD<Tuple2<Integer, String>> parallelize = jsc.parallelize(cc);
        JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> stringIterableJavaPairRDD = parallelize.groupBy(new Function<Tuple2<Integer, String>, String>() {

            @Override
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1 > 3 ? "大于3" : "小于3";
            }
        });
        stringIterableJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Tuple2<Integer, String>>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Tuple2<Integer, String>>> t) throws Exception {
                System.out.println(t._1+"======="+t._2);
            }
        });
        //=======================keyby==========================
        List<String> strings = Arrays.asList("a_sss", "a_ccxx", "b_lll", "c_alsdas", "d_sads", "c_asdw", "b_asdas");
        JavaRDD<String> parallelize1 = jsc.parallelize(strings);
        JavaPairRDD<Object, String> objectStringJavaPairRDD = parallelize1.keyBy(new Function<String, Object>() {
            @Override
            public Object call(String v1) throws Exception {
                return v1.substring(0, 1);
            }
        });
        objectStringJavaPairRDD.foreach(new VoidFunction<Tuple2<Object, String>>() {
            @Override
            public void call(Tuple2<Object, String> t) throws Exception {
                System.out.println(t._1+","+t._2);
            }
        });

        //===========================countby==================
        List<Tuple2<Integer,String>> cccc=new ArrayList<>();
        cccc.add(new Tuple2<>(1,"sss"));
        cccc.add(new Tuple2<>(2,"sss"));
        cccc.add(new Tuple2<>(1,"sss"));
        cccc.add(new Tuple2<>(3,"sss"));
        cccc.add(new Tuple2<>(4,"sss"));
        System.out.println(jsc.parallelizePairs(cccc).countByKey());
    }

}
