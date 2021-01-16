package Employee;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	//Input Key: Line Number : LongWritable
	//Input Value: Content of Each Line: Text
	//Output Key: Emp Name: Text
	//Output Value: Salary: IntWritable
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		String inputLine=value.toString().trim();
		String[] strArray=inputLine.split("\\|");
		int i=Integer.parseInt(strArray[2]);
		String name=strArray[1];
		
		if(strArray[3].equals("Accounting")) {
			context.write(new Text(name),new IntWritable(i));
		}

	}

}
