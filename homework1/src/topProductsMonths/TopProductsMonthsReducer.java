package topProductsMonths;

import homeworkUtilities.ProductWritable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopProductsMonthsReducer extends
Reducer<Text, ProductWritable, Text, Text> {


	@Override
	public void reduce(Text key, Iterable<ProductWritable> values, Context context)
			throws IOException, InterruptedException {

		Map<Text, LinkedList<Double>> productScore = new HashMap<>();

		for (ProductWritable product : values) {
			Text id = product.getId();
			if(productScore.containsKey(id)){
				LinkedList<Double> scores = productScore.get(id);
				scores.add(product.getScore());
				productScore.put(id, scores);
			} else {
				LinkedList<Double> scores = new LinkedList<>();
				scores.add(product.getScore());
				productScore.put(id, scores);
			}
		}



		List<ProductWritable> productAvg = new LinkedList<>();

		for(Entry<Text, LinkedList<Double>> entry : productScore.entrySet()) {
			double sum=0;
			LinkedList<Double> scores= entry.getValue();
			for (Double d : scores){
				sum+=d.doubleValue();
			}
			double avg = sum/(double)scores.size();
			productAvg.add(new ProductWritable(entry.getKey(), avg));
			//productAvg.a
		}

		Collections.sort(productAvg);
		/* stampa i primi 5 */
		for(int i=0; i<5; i++){
			if(!productAvg.isEmpty()){
				ProductWritable p = productAvg.get(i);
				context.write(key, new Text(p.toString()));
			}
		}
	}
}
