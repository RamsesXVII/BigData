package task2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import homeworkUtilities.ProductWritable;


public class UserFavoriteProductsMapper extends
Mapper<LongWritable, Text, Text, ProductWritable> {

public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	
	String line = value.toString();
	String[] token = line.split("\\t");
	
	Text productID= new Text(token[1]);
	Text userID= new Text(token[2]);
	Double score= Double.parseDouble(token[6]);
		
	ProductWritable product = new ProductWritable(productID, score);
	context.write(userID, product);
	}
}
