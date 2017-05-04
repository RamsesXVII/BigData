package task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Map preliminare: costruisce una mappa del tipo prodotto->utente
 * @author iori
 *
 */

public class SimilarCustomerPreliminarMapper extends
	Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] token = line.split("\\t");

		Double score= Double.parseDouble(token[6]);

		Text productID= new Text(token[1]);
		Text userId= new Text(token[2]);

		if(score>=4)
			context.write(productID, userId);
	}
}