package Employee;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * PROBLEM STATEMENT: PROCESS FILE WHICH HAS EMPID, EMPNAME,SALARY AND DEPT COLUMNS
 * 1. FIND OUT THE EMPLOYEE WITH THEIR SALARY WHO ARE WORKING IN 'ACCOUNTING' DEPARTMENT, SINCE IN THIS PROBLEM AGGREGATION IS NOT 
 * REQUIRED, SO WE WILL BE USING IDENTITY REDUCER CONCEPT.
 * 
 */
public class EmployeeDriver {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(EmployeeDriver.class);
		job.setMapperClass(EmployeeMapper.class);
		job.setNumReduceTasks(1); //for Identity Reducer where Aggregation is not required
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//Mention Input File and Output File
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem fs=FileSystem.get(conf);
		//Delete the output path
		fs.delete(new Path(args[1]));
		
		//Submit Job
		job.waitForCompletion(true);
		
	}

}
