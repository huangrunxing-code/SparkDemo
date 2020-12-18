package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class AggregateByKey {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName("hex-AggregateByKey").setMaster("local[*]"));
        List<Tuple2<String,Integer>> sourceList=new ArrayList<>();
        sourceList.add(new Tuple2<>("黄",2));
        sourceList.add(new Tuple2<>("何",4));
        sourceList.add(new Tuple2<>("赵",4));
        sourceList.add(new Tuple2<>("黄",6));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("何",1));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("何",1));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("何",1));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("赵",4));
        sourceList.add(new Tuple2<>("黄",6));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("何",1));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("张",2));
        sourceList.add(new Tuple2<>("何",1));
        JavaPairRDD<String, Integer> sourcePairRDD = jsc.parallelizePairs(sourceList, 3);
        JavaPairRDD<String, Integer> resultPairRDD = sourcePairRDD.aggregateByKey(0,AggregateByKey::seqFunc, AggregateByKey::combFunc);
        resultPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + "-------" +t._2);
            }
        });
    }

    /**
     * 用于合并同一分区同一个key的value值
     * @param a
     * @param b
     * @return
     */
    private static Integer seqFunc(Integer a,Integer b){
        return a+b;
    }

    /**
     * 用于合并不同分区同一个key的value值
     * @param a
     * @param b
     * @return
     */
    private static Integer combFunc(Integer a,Integer b){
        return a+b;
    }
}
