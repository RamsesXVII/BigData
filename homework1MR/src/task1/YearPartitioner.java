package task1;

import homeworkUtilities.ProductWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<Text, ProductWritable> {
	
	@Override
	public int getPartition(Text key, ProductWritable value, int numReduceTasks) {
		
		String[] yearMonth = key.toString().split("-");
		String year = yearMonth[0];
		int yearInt = Integer.parseInt(year);
		
		return yearInt % 3;
	}
	
}
