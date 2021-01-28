package cityPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		Double totalRev=0.0;
		for(DoubleWritable x: values) {
			totalRev=totalRev+Double.parseDouble(x.toString());
		}
		context.write(key, new DoubleWritable(totalRev));
	}

}
