package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
	//Input Key: Word: Text
	//Input Value: List of Value: IntWritable
	//Output Key: Text
	//Output Value: LongWritable
		
	@Override
	protected void reduce(Text key, Iterable<IntWritable> lov,
			Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		Long count=new Long(0);
		for(IntWritable i: lov) {
			count=count+1;
		}
		context.write(key, new LongWritable(count));

	}
	

}
