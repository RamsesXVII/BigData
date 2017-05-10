package task1Clean;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import homeworkUtilities.ProductWritable;

public class TopProductsMonthsReducer extends
Reducer<Text, DoubleWritable, Text, Text> {

	Map<Text, LinkedList<ProductWritable>> mapper = new TreeMap<>();
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double sum=0;
		double i=0;
		
		for(DoubleWritable d : values) {
			sum+= d.get();
			i++;
		}
		
		String keyString = key.toString();
		String[] token = keyString.split(" ");
		Text month = new Text(token[0]);
		
		ProductWritable p = new ProductWritable(new Text(token[1]), sum/i);
		
		LinkedList<ProductWritable> plist;
		
		if(mapper.containsKey(month))
			plist = mapper.get(month);
		else
			plist = new LinkedList<>();

		plist.add(p);
		mapper.put(month, plist);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		
		for(Entry<Text, LinkedList<ProductWritable>> entry : mapper.entrySet()){
			LinkedList<ProductWritable>  productAvg = entry.getValue();
			Collections.sort(productAvg);
			
			for(int i=0; i<5; i++){
				if(productAvg.size()>i){
					ProductWritable p = productAvg.get(i);
					context.write(entry.getKey(), new Text(p.toString()));
				}
			}
		}
	}
}

