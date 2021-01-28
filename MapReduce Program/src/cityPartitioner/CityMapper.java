package cityPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		//1,    10002,cityName,Revenue
		String inputStr=value.toString().trim();
		String[] strArr=inputStr.split(",");
		String city= strArr[1];
		Double revenue=Double.parseDouble(strArr[2]);
		context.write(new Text(city), new DoubleWritable(revenue));
	}
	
	

}
