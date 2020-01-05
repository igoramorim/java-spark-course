package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MainCountWords {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
		
		JavaRDD<String> lettersOnlyRDD = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		JavaRDD<String> removedBlankLines = lettersOnlyRDD.filter(sentence -> sentence.trim().length() > 0);
		
		JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
		
		JavaRDD<String> interestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));
		
		JavaPairRDD<String, Long> pairRDD = interestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
		
		JavaPairRDD<String, Long> totals = pairRDD.reduceByKey((v1, v2) -> v1 + v2);
		
		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));
		
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
		
		List<Tuple2<Long, String>> results = sorted.take(15);
		results.forEach(System.out::println);
		
		sc.close();
		
	}

}
