package com.huangyueran.spark.hrx;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SerializationApp {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
                //.setAppName("SerializationApp").setMaster("locla[2]");
       // sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkContext sc = SparkSession.builder().config(sparkConf).getOrCreate().sparkContext();
        //SparkContext sc = new JavaSparkContext(sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        int flag=jsc.getConf().getInt("spark.flag",0);//自定义参数一定要以spark.开头
        List<Info> infos=new ArrayList<>();
        List<String> c= new ArrayList<>();
        String[] names={"PK","Jespon","huhu"};
        String[] genders={"male","female"};
        String[] addresses={"beijing","shenzhen","shanghai","guangzhou","nanning","ganzhou"};
        for(int i=0;i<100000000;i++){
            String name= names[new Random().nextInt(2)];
            int age=new Random().nextInt(100)+1;
            String gender= genders[new Random().nextInt(1)];
            String address= addresses[new Random().nextInt(5)];
            infos.add(new Info(name,i,gender,address));
        }
        JavaRDD<Info> rdd = jsc.parallelize(infos);
        if(flag==0) {
            rdd.persist(StorageLevel.MEMORY_ONLY());
        }else{
            rdd.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        System.out.println(rdd.count());
        Thread.sleep(60*1000);
        jsc.stop();
    }
}
class Info{
    private String name;
    private int age;
    private String gender;
    private String address;

    public Info(String name, int age, String gender, String address) {
        this.name = name;
        this.age = age;
        this.gender = gender;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
