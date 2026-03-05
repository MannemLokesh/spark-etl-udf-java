package com.digthetechnology.apachespark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

public class RddsOfObjects {

	public static void main(String[] args) {

//		SparkConf conf = new SparkConf().setAppName("MappingOperations").setMaster("local[*]");
//
//		JavaSparkContext sc = new JavaSparkContext(conf);
//
//		// Creating RDD from a List
//		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
//
//		JavaRDD<IntegerWithSquareRoot> sqrtRdd = rdd.map(value -> new IntegerWithSquareRoot(value));
//
//		sqrtRdd.foreach(value -> System.out.println(value));
//
//		System.out.println("Length of RDD: " + sqrtRdd.count());
//
//		sc.close();
		
		
		SparkSession spark = SparkSession.builder()
		        .appName("AggExample")
//		        .master("local[*]")
		        .master("spark://192.168.129.6:7077")
		        .getOrCreate();
		
//		spark.udf().register("ADD", (Seq<Double> values) -> {
//		    if (values == null || values.isEmpty()) return null;
//
//		    java.util.List<Double> list = CollectionConverters.asJava(values);
//
//		    double sum = 0.0;
//		    for (Double v : list) {
//		        if (v != null) sum += v;
//		    }
//		    return sum;
//		}, DataTypes.DoubleType);
//
//		
//		spark.udf().register("SUB", (Seq<Double> values) -> {
//		    if (values == null || values.isEmpty()) return null;
//
//		    // Convert Scala Seq to Java List
//		    java.util.List<Double> list = CollectionConverters.asJava(values);
//
//		    double result = list.get(0);
//		    for (int i = 1; i < list.size(); i++) {
//		        Double v = list.get(i);
//		        if (v != null) result -= v;
//		    }
//		    return result;
//		}, DataTypes.DoubleType);
		
		// SUB UDF
		spark.udf().register("SUB", (Seq<Number> values) -> {
		    double result = CollectionConverters.asJava(values).get(0).doubleValue();
		    for (int i = 1; i < values.size(); i++) {
		        Number v = CollectionConverters.asJava(values).get(i);
		        if (v != null) result -= v.doubleValue();
		    }
		    return result;
		}, DataTypes.DoubleType);

		spark.udf().register("ADD", (Seq<Number> values) -> {
		    double sum = 0.0;
		    for (Number v : CollectionConverters.asJava(values)) {
		        if (v != null) sum += v.doubleValue();
		    }
		    return sum;
		}, DataTypes.DoubleType);

		
		spark.udf().register("MUL", (List<Double> values) -> {
		    double result = 1.0;
		    for (Double v : values)
		        if (v != null) result *= v;
		    return result;
		}, DataTypes.DoubleType);

		
//		spark.udf().register("SUB", (List<Double> values) -> {
//		    if (values == null || values.isEmpty()) return null;
//
//		    double result = values.get(0);
//		    for (int i = 1; i < values.size(); i++)
//		        if (values.get(i) != null)
//		            result -= values.get(i);
//
//		    return result;
//		}, DataTypes.DoubleType);

		
		List<Row> data = Arrays.asList(
                RowFactory.create(10, 3, 2, 1),
                RowFactory.create(20, 5, 3, 2),
                RowFactory.create(15, 5, 1, 4)
        );
		
		StructType schema = new StructType()
                .add("col1", DataTypes.IntegerType)
                .add("col2", DataTypes.IntegerType)
                .add("col3", DataTypes.IntegerType)
                .add("col4", DataTypes.IntegerType);
		
		 Dataset<Row> df = spark.createDataFrame(data, schema);
	      df.show();
		
	      
	      String sql = "SELECT ADD(array(SUB(array(col1, col2, 5)), col3, col4, 10)) AS result FROM df";
	        df.createOrReplaceTempView("df");
	        
	        Dataset<Row> result = spark.sql(sql);
	        result.show();
		//df.selectExpr(sql).show();
	        
//	        +------+
//	        |result|
//	        +------+
//	        |     5|   // (10-3-5) + 2 + 1 = 5
//	        |    15|   // (20-5-5) + 3 + 2 = 15 + 3 + 2 = 20
//	        |     10|   // (15-5-5) + 1 + 4 = 5 + 1 + 4 = 10 ??? let's check
//	        +------+
	        
	        try {
				Thread.sleep(1000000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

}
