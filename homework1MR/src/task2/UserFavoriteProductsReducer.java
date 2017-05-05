package task2;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import homeworkUtilities.ProductWritable;

public class UserFavoriteProductsReducer extends
Reducer<Text, ProductWritable, Text, Text> {


	@Override
	public void reduce(Text key, Iterable<ProductWritable> values, Context context)
			throws IOException, InterruptedException {

		List<ProductWritable> userProducts = new LinkedList<>();

		for(ProductWritable p : values){
			ProductWritable pnew= new ProductWritable(p.getId(), p.getScore());
			userProducts.add(pnew);

		}	
		
		String result ="\t";

		
		Collections.sort(userProducts);
		

		/* stampa i primi 5 */
		for(int i=0; i<10; i++){
			if((userProducts.size()>i)){
				ProductWritable p = userProducts.get(i);
				result = result + "\"" + p.getId() + "-" + p.getScore() +"\",";
			}
		}
		context.write(key, new Text(result));
	}
}

