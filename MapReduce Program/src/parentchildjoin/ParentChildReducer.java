package parentchildjoin;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParentChildReducer extends Reducer<IntWritable, Text, Text, Text>{

	@Override
	protected void reduce(IntWritable arg0, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//Input: {1, {child Name#Child, Parent Name#Parent,...}}
		
		int childCount=0;
		String ParentName="";
		Vector v= new Vector();
		
		for(Text x: values) {
			if(x.toString().trim().split("#")[1].trim().toUpperCase().equals("PARENT")) {
				ParentName=x.toString().trim().split("#")[0];
			}
			if(x.toString().trim().split("#")[1].trim().toUpperCase().equals("CHILD")) {
				v.addElement(x.toString().trim().split("#")[0]);
			}
		}
		if(ParentName.length()==0) {
			ParentName="No Parent";
		}
		for(Object y: v) {
			context.write(new Text(ParentName), new Text(y.toString().trim()));
		}
		if(v.isEmpty()) {
			context.write(new Text(ParentName), new Text("No Children"));
		}
	}

}
