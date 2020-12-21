package com.huangyueran.spark.hrx.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Join {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setAppName(Join.class.getName()).setMaster("local[*]"));
        List<Tuple2<Integer,String>> al=new ArrayList<>();
        List<Tuple2<Integer,String>> bl=new ArrayList<>();
        al.add(new Tuple2<>(1,"this is al 1"));
        al.add(new Tuple2<>(3,"this is al 3"));
        al.add(new Tuple2<>(5,"this is al 5"));
        al.add(new Tuple2<>(7,"this is al 7"));
        al.add(new Tuple2<>(0,"this is al 0"));


        bl.add(new Tuple2<>(2,"this is bl 2"));
        bl.add(new Tuple2<>(4,"this is bl 3"));
        bl.add(new Tuple2<>(6,"this is bl 6"));
        bl.add(new Tuple2<>(8,"this is bl 8"));
        bl.add(new Tuple2<>(0,"this is bl 0"));

        JavaPairRDD<Integer, String> ardd = jsc.parallelizePairs(al);
        JavaPairRDD<Integer, String> brdd = jsc.parallelizePairs(bl);

        System.out.println("====================inner join=============================");
        JavaPairRDD<Integer, Tuple2<String, String>> joinrdd = ardd.join(brdd);
          joinrdd.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, String>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, String>> t) throws Exception {
                System.out.println(t._1+","+t._2);
            }
        });
        System.out.println("=====================left join============================");
        JavaPairRDD<Integer, Tuple2<String, Optional<String>>> leftrdd = ardd.leftOuterJoin(brdd);
        leftrdd.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Optional<String>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Optional<String>>> t) throws Exception {
                System.out.println(t._1+","+t._2);
            }
        });
        System.out.println("===================right join==============================");
        JavaPairRDD<Integer, Tuple2<Optional<String>, String>> rightrdd = ardd.rightOuterJoin(brdd);
        rightrdd.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<String>, String>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Optional<String>, String>> t) throws Exception {
                System.out.println(t._1+","+t._2);
            }
        });
        System.out.println("===================full out join==============================");
        JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<String>>> outerJoin = ardd.fullOuterJoin(brdd);
        outerJoin.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<String>, Optional<String>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Optional<String>, Optional<String>>> t) throws Exception {

                System.out.println(t._1 + "," + t._2);
            }
        });

    }
}
