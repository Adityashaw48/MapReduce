package callAnalysis;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class callDriver {
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job= new Job(conf);
		job.setJarByClass(callDriver.class);
		DistributedCache.addCacheFile(new URI(args[0]), job.getConfiguration());
		job.setMapperClass(callMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(callReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		FileSystem fs=FileSystem.get(conf);
		fs.delete(new Path(args[2]));
		job.waitForCompletion(true);
		
	}

}
