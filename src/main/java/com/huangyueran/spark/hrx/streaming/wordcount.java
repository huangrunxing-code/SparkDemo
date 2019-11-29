package com.huangyueran.spark.hrx.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 功能描述:
 *
 * @ClassName: wordcount
 * @Author: huangrx丶
 * @Date: 2019/11/28 20:11
 * @Version: V1.0
 */
public class wordcount {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf()
                .setAppName("wordcount")
                .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("hadoop1", 9999);

        JavaDStream<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairsRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairDStream<String, Integer> resultRDD = pairsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i, Integer i2) throws Exception {
                return i+i2;
            }
        });




       /* resultRDD.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                stringIntegerJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        System.out.println(stringIntegerTuple2._1+"，"+stringIntegerTuple2._2);
                    }
                });
            }
        });*/

        Thread.sleep(5);


        resultRDD.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
