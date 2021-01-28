package cityPartitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CityPartitioner extends Partitioner<Text, DoubleWritable> {

	@Override
	public int getPartition(Text key, DoubleWritable value, int ret) {
		// Input: City, Revenue
		String inputStr=key.toString().trim();
		if(inputStr.equals("Delhi")) {
			return 0;
		}
		if(inputStr.equals("Bangalore")) {
			return 1;
		}
		if(inputStr.equals("Chennai")) {
			return 1;
		}
		if(inputStr.equals("Mumbai")) {
			return 0;
		}
		if(inputStr.equals("Kolkata")) {
			return 2;
		}
		else {
			return 2;
		}
		
	}
	

}
