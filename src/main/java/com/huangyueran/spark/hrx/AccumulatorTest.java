package com.huangyueran.spark.hrx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class AccumulatorTest {

    //累加变量

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Accumulator")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        List<Long> integers = Arrays.asList(1l, 2l, 3l, 4l, 5l);
        //2.x之后的版本要这样创建
        final LongAccumulator longAccumulator = sc.sc().longAccumulator();

        JavaRDD<Long> parallelize = sc.parallelize(integers);
        parallelize.foreach(new VoidFunction<Long>() {
            @Override
            public void call(Long integer) throws Exception {
                System.out.println(longAccumulator.value());
                    longAccumulator.add(integer);
            }
        });

        System.out.println(longAccumulator.value());

        sc.close();

    }
}
