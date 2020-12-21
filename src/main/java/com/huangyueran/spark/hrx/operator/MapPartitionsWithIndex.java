package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsWithIndex {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(MapPartitionsWithIndex.class.getName()).setMaster("local[*]"));
        List<String> strings = Arrays.asList("a1", "b1", "c1", "a2", "b2", "c2", "a3", "b3", "c3");
        JavaRDD<String> parallelize = jsc.parallelize(strings, 3);
        List<String> iterator = parallelize.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                List<String> al = new ArrayList<>();
                while (v2.hasNext()) {
                    al.add("index : " + v1 + "value: " + v2);
                }
                return al.iterator();
            }
        },true).collect();
//        List<String> collect = parallelize.mapPartitionsWithIndex(new FlatMapFunction<Iterator<String>, String>() {
//            int num = 1;//每个分区内单独计数
//
//            @Override
//            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
//                List<String> ar = new ArrayList<>();
//                while (stringIterator.hasNext()) {
//                    String next = stringIterator.next();
//                    ar.add("count: " + (num++) + "\t" + next);
//                }
//                return ar.iterator();
//            }
//        }).collect();

        for(String cc : collect){
            System.out.println(cc);
        }
    }
}
