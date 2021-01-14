package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	//Input Key: Line Number : LongWritable
	//Input Value: Line Content: Text
	//Output Key: Word: Text
	//Output Value: 1: IntWritable
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String inputText=value.toString();
		String[] strArray=inputText.split(" ");
		
		for(String word: strArray) {
			context.write(new Text(word),new IntWritable(1));
		}
	}

}
