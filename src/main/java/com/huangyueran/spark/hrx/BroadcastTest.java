package com.huangyueran.spark.hrx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class BroadcastTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("broadcast")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5);

        final Integer facts=3;
        final Broadcast<Integer> broadcast = sc.broadcast(facts);


        JavaRDD<Integer> parallelize = sc.parallelize(integers);
        JavaRDD<Integer> map = parallelize.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer integer) throws Exception {
                int fct=broadcast.getValue();
                return integer*fct;
            }
        });

        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer o) throws Exception {
                System.out.println(o);
            }
        });
        sc.close();

    }


}
