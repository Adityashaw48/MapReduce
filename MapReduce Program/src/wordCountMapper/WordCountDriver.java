package wordCountMapper;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf= new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setNumReduceTasks(0);
		//job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//----Common to all code----\\
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
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
