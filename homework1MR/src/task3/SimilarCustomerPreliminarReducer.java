package task3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarCustomerPreliminarReducer extends
Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		Set<String>userSet= new TreeSet<String>(); //Treeset TODO
		Set<String>utilityUserSet=new HashSet<String>();
		
		for(Text userId : values)
			userSet.add(userId.toString());
	
		utilityUserSet.addAll(userSet);


		for (String firstUserId : userSet) {
			
			for(String secondUserId : utilityUserSet){
				if(!firstUserId.equals(secondUserId)){
					context.write(new Text(firstUserId+"-"+secondUserId),key);
				}
			}
			utilityUserSet.remove(firstUserId); //va messo in qualche modo, ma così ses capita una vola prima
		}
		
		

//		for (String firstUserId : userSet) {
//			utilityUserSet.add(firstUserId);
//			for(String secondUserId : userSet){
//				if(!utilityUserSet.contains(secondUserId)){
//					context.write(new Text(firstUserId+"-"+secondUserId),key);
//				}
//			}
//		}
	}
}