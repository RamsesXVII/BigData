package topProductsMonths;

import homeworkUtilities.ProductWritable;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopProductsMonthsMapper extends
	Mapper<LongWritable, Text, Text, ProductWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] token = line.split("\\t");
		
		Text productID= new Text(token[1]);
		Double score= Double.parseDouble(token[6]);
		
		Long timestamp = Long.parseLong(token[7]);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		String date = sdf.format(timestamp*1000);
		Text time = new Text(date);		
		ProductWritable product = new ProductWritable(productID, score);
		context.write(time, product);
		}
	}
