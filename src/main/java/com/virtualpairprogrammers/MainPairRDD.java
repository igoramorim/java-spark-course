package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class MainPairRDD {

	public static void main(String[] args) {

		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// normal method
//		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
//		
//		JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(rawValue -> {
//			String[] columns = rawValue.split(":");
//			String level = columns[0];
//			
//			return new Tuple2<>(level, 1L);
//		});
//		
//		JavaPairRDD<String, Long> sumRdd = pairRdd.reduceByKey((v1, v2) -> v1 + v2);
//		
//		sumRdd.collect().forEach((tuple -> {
//			System.out.println(tuple._1 + " has " + tuple._2 + " instances");
//		}));
		
		
		// fluent method
		sc.parallelize(inputData)
		  .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
		  .reduceByKey((v1, v2) -> v1 + v2)
		  .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		
		// groupByKey version
		// groupByKey has performance issues in large datasets
//		sc.parallelize(inputData)
//		  .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
//		  .groupByKey()
//		  .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));
		
		sc.close();
		
	}

}
