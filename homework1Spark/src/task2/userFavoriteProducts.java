package task2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class  userFavoriteProducts { 
	public static void main(String[] args) { 
		String logFile = "/home/mattia/Scaricati/amazon/1999_2006.csv";
		SparkConf conf = new SparkConf().setAppName("task 2 Application"); 


		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logData = sc.textFile(logFile).cache(); 

		JavaPairRDD<String, String> userProducts = logData.mapToPair(
				new PairFunction<String, String, String>(){
					@Override
					public Tuple2<String, String> call(String s){
						String[] splitted = s.split("\\t");
						return new Tuple2<>(splitted[1], splitted[2]+ ";"+ splitted[3]);
					}
				});
		List<Tuple2<String, String>> output = userProducts.collect();
		for(Tuple2<?,?> t: output){
			System.out.println(t._1() + "; " + t._2());
		}
		sc.close();
	}
}
