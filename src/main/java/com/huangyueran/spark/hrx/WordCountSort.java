package com.huangyueran.spark.hrx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 实现对单词出现次数进行统计，并且按照次数降序排序
 */
public class WordCountSort {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountSort")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> testRDD = sc.textFile("C://Users//Administrator//Desktop//temp.txt");

        JavaRDD<String> stringJavaRDD = testRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] splits = s.split("\\,", -1);
                return Arrays.asList(splits).iterator();
            }
        });

        JavaPairRDD<String, Integer> word1RDD = testRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairRDD<String, Integer> reduceRDD = word1RDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        JavaPairRDD< Integer, String> pairRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2< Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2< Integer, String>(t._2,t._1);
            }
        });


        JavaPairRDD<Integer, String> sortRDD = pairRDD.sortByKey(false);

        JavaPairRDD<String, Integer> resultRDD = sortRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2,t._1);
            }
        });

        resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();

    }
}
