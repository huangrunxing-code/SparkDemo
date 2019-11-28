package com.huangyueran.spark.hrx;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOpration {
    public static void main(String[] args) {
        // map();

        // filter();

        //flatmap();
        //groupbykey();
        //reducebykey();
        //sortkey();
        //join();
        cogroup();

    }


    public static void map(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("map")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Integer> numbers=Arrays.asList(2,3,5,6,7);

        JavaRDD<Integer> numbersRdd = jsc.parallelize(numbers);

        JavaRDD<Integer> mapRdd = numbersRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 3;
            }
        });

        mapRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        jsc.close();
    }

    public static void filter(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("filter")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Integer> numbers=Arrays.asList(1,2,3,4,5,6,7,8,9,10,0);

        JavaRDD<Integer> numberRdd = jsc.parallelize(numbers);

        JavaRDD<Integer> filterrdd = numberRdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2==0;
            }
        });

        filterrdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        jsc.close();

    }



    public static void flatmap(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("flatmap")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<String> lines=Arrays.asList("ni hao","wo shi shui","ni shi sha bi");
        JavaRDD<String> parallelize = jsc.parallelize(lines);
        JavaRDD<String> stringJavaRDD = parallelize.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return   Arrays.asList(s.split(" ")).iterator();
            }
        });

        stringJavaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        jsc.close();

    }
    
    private static void reducebykey(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("reducebykey")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<String, Integer>("huangtun", 76),
                new Tuple2<String, Integer>("lihs", 45),
                new Tuple2<String, Integer>("njs", 96),
                new Tuple2<String, Integer>("huangtun", 76),
                new Tuple2<String, Integer>("lihs", 70),
                new Tuple2<String, Integer>("njs", 56));
        JavaPairRDD<String, Integer> scoresRDD = jsc.parallelizePairs(scores);

        JavaPairRDD<String, Integer> sumRDD = scoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        sumRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });
        jsc.close();

    }
    

    private static void sortkey(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("sortkey")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, String>> scores = Arrays.asList(new Tuple2<Integer, String>(80, "a"),
                new Tuple2<Integer, String>(60, "f"),
                new Tuple2<Integer, String>(75, "v"),
                new Tuple2<Integer, String>(97, "c"),
                new Tuple2<Integer, String>(82, "r"),
                new Tuple2<Integer, String>(84, "x"),
                new Tuple2<Integer, String>(92, "l")
        );
        JavaPairRDD<Integer, String> scoresRDD = jsc.parallelizePairs(scores);

        JavaPairRDD<Integer, String> sortRDD = scoresRDD.sortByKey(false);
        sortRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._2+":"+t._1);
            }
        });
        jsc.close();
    }
    
    
    private static void groupbykey(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("groupbykey")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<String, Integer>("huangtun", 76),
                new Tuple2<String, Integer>("lihs", 45),
                new Tuple2<String, Integer>("njs", 96),
                new Tuple2<String, Integer>("huangtun", 76),
                new Tuple2<String, Integer>("lihs", 70),
                new Tuple2<String, Integer>("njs", 56));
        JavaPairRDD<String, Integer> scoresRDD = jsc.parallelizePairs(scores);

        JavaPairRDD<String, Iterable<Integer>> groupRDD = scoresRDD.groupByKey();

        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);
                Iterator<Integer> i = t._2.iterator();
                while (i.hasNext()){
                    System.out.println(i.next());
                }
                System.out.println("=======================");
            }
        });


        jsc.close();
        

    }



    private static void join(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("join")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, String>> names = Arrays.asList(new Tuple2<Integer, String>(1, "huangur"),
                new Tuple2<Integer, String>(2, "lihud"),
                new Tuple2<Integer, String>(3, "asdr"),
                new Tuple2<Integer, String>(4, "aope"),
                new Tuple2<Integer, String>(9, "ssssaope"));


        List<Tuple2<Integer, Integer>> scores = Arrays.asList(new Tuple2<Integer, Integer>(1, 82),
                new Tuple2<Integer, Integer>(2, 66),
                new Tuple2<Integer, Integer>(3, 77),
                new Tuple2<Integer, Integer>(4, 86),
                new Tuple2<Integer, Integer>(5, 89));

        JavaPairRDD<Integer, String> namesRDD = jsc.parallelizePairs(names);
        JavaPairRDD<Integer, Integer> scoresRDD = jsc.parallelizePairs(scores);

        //leftjoin
       // JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> integerTuple2JavaPairRDD = namesRDD.leftOuterJoin(scoresRDD);


        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = namesRDD.join(scoresRDD);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println(t._1+"|"+t._2._1+"|"+t._2._2);
            }
        });



        jsc.close();

    }



    private static void cogroup(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("join")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, String>> names = Arrays.asList(new Tuple2<Integer, String>(1, "huangur"),
                new Tuple2<Integer, String>(2, "lihud"),
                new Tuple2<Integer, String>(3, "asdr"),
                new Tuple2<Integer, String>(4, "aope"),
                new Tuple2<Integer, String>(9, "ssssaope"));


        List<Tuple2<Integer, Integer>> scores = Arrays.asList(new Tuple2<Integer, Integer>(1, 82),
                new Tuple2<Integer, Integer>(2, 66),
                new Tuple2<Integer, Integer>(3, 77),
                new Tuple2<Integer, Integer>(4, 86),
                new Tuple2<Integer, Integer>(5, 89),
                new Tuple2<Integer, Integer>(4, 77),
                new Tuple2<Integer, Integer>(5, 45));

        JavaPairRDD<Integer, String> namesRDD = jsc.parallelizePairs(names);
        JavaPairRDD<Integer, Integer> scoresRDD = jsc.parallelizePairs(scores);

        //leftjoin
        // JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> integerTuple2JavaPairRDD = namesRDD.leftOuterJoin(scoresRDD);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = namesRDD.cogroup(scoresRDD);


        cogroupRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println(t._1);
                Iterator<String> names = t._2._1.iterator();
                Iterator<Integer> scores = t._2._2.iterator();
                while(names.hasNext()){
                    System.out.println(names.next());
                }
                while(scores.hasNext()){
                    System.out.println(scores.next());
                }

            }
        });


        jsc.close();

    }
}
