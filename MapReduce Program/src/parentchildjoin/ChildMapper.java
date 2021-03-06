package parentchildjoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChildMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String inputText=value.toString().trim();
		String ChildName=inputText.split(",")[1];
		Integer PID= Integer.parseInt(inputText.split(",")[2]);
		context.write(new IntWritable(PID), new Text(ChildName+"#"+"Child"));
	}
	
	

}
