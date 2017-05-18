package task1Clean;


import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopProductsMonths {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "TopProductsMonthsCleanUp");

		job.setJarByClass(TopProductsMonths.class);
		
		job.setMapperClass(TopProductsMonthsMapper.class);
		job.setReducerClass(TopProductsMonthsReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setPartitionerClass(YearPartitioner.class);
		//job.setNumReduceTasks(3);

		long start = Instant.now().toEpochMilli();
		job.waitForCompletion(true);
		long end = Instant.now().toEpochMilli();
		System.out.println(end - start);
	}
}