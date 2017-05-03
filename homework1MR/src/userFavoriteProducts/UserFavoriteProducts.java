package userFavoriteProducts;

import homeworkUtilities.ProductWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserFavoriteProducts {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "userFavoriteProducts");

		job.setJarByClass(UserFavoriteProducts.class);
		
		job.setMapperClass(UserFavoriteProductsMapper.class);
		job.setReducerClass(UserFavoriteProductsReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProductWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}