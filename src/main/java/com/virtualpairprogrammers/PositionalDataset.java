package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.virtualpairprogrammers.model.Layout;
import com.virtualpairprogrammers.model.Line;

public class PositionalDataset {
	
	public static Layout layout;

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("PositionalDataset").getOrCreate();
		
//		Dataset<Row> dataset = getDataset(spark);
		Dataset<Row> dataset = getDatasetFromTxt(spark);
		dataset.show(false);
		dataset.printSchema();
		
		dataset = castDataTypeToString(dataset);
		
		JavaRDD<String> cleanBrackets = dataset.javaRDD().coalesce(1).map(l -> l.toString().replace("[", "").replace("]", ""));
		cleanBrackets.collect().forEach(System.out::println);
		
		layout = loadLayout();
		
		JavaRDD<String> map = cleanBrackets.map(formatLine);
		map.collect().forEach(System.out::println);
		
		map.saveAsTextFile("/home/igor/positional.txt");
		
//		dataset.javaRDD().coalesce(1).saveAsTextFile("/home/igor/positional.txt");
//		dataset.write().format("text").save("/home/igor/positional.txt");
//		dataset.write().text("/home/igor/positional.txt");
		
	}
	
	private static Function<String, String> formatLine = line -> {
		String[] values = line.split(",");
		// Put it in another method
		String[] lineValues = Arrays.stream(values).filter(x -> StringUtils.isNotEmpty(x)).toArray(String[]::new);
		// Put it in another method
		Line lineLayout = layout.getLines().stream()
			.filter(x -> lineValues[0].equalsIgnoreCase(x.getType()))
			.collect(Collectors.toList())
			.get(0);
		
		for (int i = 0; i < lineValues.length; i++) {
			if (lineLayout.getFields().get(i).getPad().equalsIgnoreCase("left")) {
				lineValues[i] = StringUtils.leftPad(
						lineValues[i], lineLayout.getFields().get(i).getSize(), lineLayout.getFields().get(i).getFill());
			} else {
				lineValues[i] = StringUtils.rightPad(
						lineValues[i], lineLayout.getFields().get(i).getSize(), lineLayout.getFields().get(i).getFill());
			}
		}
		return String.join("", lineValues);
	};
	

	
	private static Dataset<Row> castDataTypeToString(Dataset<Row> dataset) {
		for(String col : dataset.columns()) {
			dataset = dataset.withColumn(col, col(col).cast(DataTypes.StringType));
		}
		dataset.show(false);
		dataset.printSchema();
		return dataset;
	}
	
	private static Dataset<Row> getDataset(SparkSession spark) {
		List<Row> data = Arrays.asList(
				RowFactory.create("T0", 1L, "igor", 26, "amorim"),
				RowFactory.create("T1", 2L, "maria", 86, "menezes"),
				RowFactory.create("T2", 3L, "joao", 88, "menezes")
		);
		
		StructType schema = new StructType(new StructField[] {
				new StructField("type", DataTypes.StringType, false,Metadata.empty()),
				new StructField("id", DataTypes.LongType, false,Metadata.empty()),
				new StructField("nome", DataTypes.StringType, false,Metadata.empty()),
				new StructField("idade", DataTypes.IntegerType, false,Metadata.empty()),
				new StructField("sobrenome", DataTypes.StringType, false,Metadata.empty())
		});

		Dataset<Row> dataset = spark.createDataFrame(data, schema);
		return dataset;
	}
	
	private static Dataset<Row> getDatasetFromTxt(SparkSession spark) {
	Dataset<Row> dataset = spark.read()
				.format("csv")
				.option("delimiter", ",")
				.load("src/main/resources/positionaldataset/input.txt");
		return dataset;
	}
	
	private static Layout loadLayout() throws JsonParseException, JsonMappingException, IOException {
		InputStream input = Layout.class.getClassLoader().getResourceAsStream("positionaldataset/layout.json");
		ObjectMapper mapper = new ObjectMapper();
//		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		Layout layout = mapper.readValue(input, Layout.class);
		String prettyPrint = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(layout);
//		System.out.println(prettyPrint);
		return layout;
	}

}
