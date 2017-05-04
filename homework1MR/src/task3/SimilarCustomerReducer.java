package task3;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarCustomerReducer  extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		HashSet<String>productList= new HashSet<String>();

		for(Text productId: values)
			productList.add(productId.toString());

		if(productList.size()>2){
			String listOfproducts= "";
			for(String productId: productList){
				listOfproducts= listOfproducts+productId+"-";
			}

			context.write(new Text(key.toString()), new Text(listOfproducts.substring(0, listOfproducts.lastIndexOf("-"))));
		}
	}
}
