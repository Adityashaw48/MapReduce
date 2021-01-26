package callAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.filecache.DistributedCache;

public class callMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	HashMap<Integer,String> CountryMaster=new HashMap<Integer,String>();
	//Array of Key,Value Pair
	//1,US
	//2,Australia and so on...
	
	
	//Setup Method Called only once before Mapper
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		Path[] p = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for(Path x: p) {
			//Only One File: Country Code File
			BufferedReader br= new BufferedReader(new FileReader(x.toString()));
			String inpstr=br.readLine();
			while(inpstr!=null) {
				CountryMaster.put(Integer.parseInt(inpstr.trim().split(",")[0].trim()), inpstr.split(",")[1].toUpperCase().trim());
				//1 US
				//2 AUSTRALIA
				inpstr=br.readLine();
			}
		}
		
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		//Input 
		//Key Value
		//1 98765432,98764324,15,1
		
		String inputStr=value.toString().trim();
		String[] inputArr=inputStr.split(",");
		Integer Cid=Integer.parseInt(inputArr[3].trim());
		Double cd=Double.parseDouble(inputArr[2].trim());
		if(CountryMaster.containsKey(Cid)) {
			if(!CountryMaster.get(Cid).toUpperCase().trim().equals("INDIA")) {
				context.write(new Text(CountryMaster.get(Cid).toUpperCase()), new DoubleWritable(cd));
			}
			//AUSTRALIA 23
		}
		
	}

}
