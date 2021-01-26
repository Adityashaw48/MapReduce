package callAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class callReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		Double cd=0.00;
		for(DoubleWritable x: values) {
			cd=cd+Double.parseDouble(x.toString());
		}
		context.write(key, new DoubleWritable(cd));
	}
	
	

}
