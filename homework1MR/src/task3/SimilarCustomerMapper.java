package task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarCustomerMapper extends
Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		
		String line = value.toString();
		String[] token = line.split("\\t");
		
		context.write(new Text(token[0]),new Text(token[1]));
		
	}
}