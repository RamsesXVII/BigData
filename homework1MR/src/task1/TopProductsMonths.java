package task1;


import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import homeworkUtilities.ProductWritable;

public class TopProductsMonths {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "TopProductsMonths");

		job.setJarByClass(TopProductsMonths.class);
		
		job.setMapperClass(TopProductsMonthsMapper.class);
		job.setReducerClass(TopProductsMonthsReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProductWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		long start = Instant.now().toEpochMilli();
		job.waitForCompletion(true);
		long end = Instant.now().toEpochMilli();
		System.out.println(end - start);
	}
}