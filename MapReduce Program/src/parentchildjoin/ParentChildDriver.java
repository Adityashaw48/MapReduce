package parentchildjoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParentChildDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job= new Job();
		job.setJarByClass(ParentChildDriver.class);
		
		//File Structure: ParentId,ParentName,Age [Delimiter: ',']
		MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,ParentMapper.class);
		//File Structure: ChildrenId,ChildrenName,ParentId [Delimiter: ',']
		MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,ChildMapper.class);
		job.setReducerClass(ParentChildReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		FileSystem fs= FileSystem.get(conf);
		fs.delete(new Path(args[2]));
		job.waitForCompletion(true);
		
	}

}
