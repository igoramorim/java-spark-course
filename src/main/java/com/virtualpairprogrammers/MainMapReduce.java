package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MainMapReduce {
	
	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<Integer>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("Spark Course").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> rdd = sc.parallelize(inputData);
		
		Integer rddSum = rdd.reduce((v1, v2) -> v1 + v2);
		System.out.println(rddSum);

		JavaRDD<Double> rddSqrt = rdd.map(v -> Math.sqrt(v));
		// rddSqrt.foreach(v -> System.out.println(v));
		rddSqrt.collect().forEach(System.out::println);
		
		System.out.println(rddSqrt.count());
		
		// Counting without the count() method
		JavaRDD<Long> sqrtToOneValue = rddSqrt.map(v -> 1L);
		Long sqrtCount = sqrtToOneValue.reduce((v1, v2) -> v1 + v2);
		System.out.println(sqrtCount);
		
		sc.close();
		
	}

}
