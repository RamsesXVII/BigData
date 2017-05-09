package task3;

import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Unmesha SreeVeni U.B
 * 
 */
public class SimilarCustomer extends Configured implements Tool {

	private static final String OUTPUT_PATH = "/output/intermediate_output";

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Job 1
		 */
		Configuration conf = getConf();

		Job job = new Job(conf, "Job1");

		job.setJarByClass(SimilarCustomer.class);

		job.setMapperClass(SimilarCustomerPreliminarMapper.class);
		job.setReducerClass(SimilarCustomerPreliminarReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class); //??
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(4);
		

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		long start = Instant.now().toEpochMilli();
		job.waitForCompletion(true);

		/*
		 * Job 2
		 */

		Job job2 = new Job(conf, "Job 2");
		job2.setJarByClass(SimilarCustomer.class);

		job2.setMapperClass(SimilarCustomerMapper.class);
		job2.setReducerClass(SimilarCustomerReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setNumReduceTasks(4);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		TextOutputFormat.setOutputPath(job2, new Path(args[1]));

		int result = job2.waitForCompletion(true) ? 0 : 1;
		long end = Instant.now().toEpochMilli();
		System.out.println(end - start);
		return result;
	}

	/**
	 * Method Name: main Return type: none Purpose:Read the arguments from
	 * command line and run the Job till completion
	 * 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(0);
		}
		ToolRunner.run(new Configuration(), new SimilarCustomer(), args);
	}
}