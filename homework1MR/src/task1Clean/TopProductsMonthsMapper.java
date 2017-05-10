package task1Clean;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import homeworkUtilities.ProductWritable;


public class TopProductsMonthsMapper extends
Mapper<LongWritable, Text, Text, DoubleWritable> {

public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	
	String line = value.toString();
	String[] token = line.split("\\t");
	
	String productID= token[1];
	Double score= Double.parseDouble(token[6]);
	Long timestamp = Long.parseLong(token[7]);
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
	String date = sdf.format(timestamp*1000);
	
	String keyLine = date + " " + productID;
	
	context.write(new Text(keyLine), new DoubleWritable(score));
	}
}
