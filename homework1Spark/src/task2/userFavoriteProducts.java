package task2;

import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class  userFavoriteProducts { 
	public static void main(String[] args) { 
		String logFile = "/home/mattia/Scaricati/amazon/1999_2006.csv";
		SparkConf conf = new SparkConf().setAppName("task 2 Application"); 


		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logData = sc.textFile(logFile).cache();
		
		JavaRDD<Tuple3<String,String,String>> x = logData.map(
				new Function<String, Tuple3<String,String,String>>() {
					public Tuple3<String, String, String> call(String s){
						return new Tuple3<>("a","a","a");
					}
				});
		
		
		
		sc.close();
	}
}
