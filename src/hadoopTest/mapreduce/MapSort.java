package hadoopTest.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapSort extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> 
{
	private Text caller = new Text();

	// key, "Coller sum", Ex "A 10" => (sum, Coller), Ex (10, "A")
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException 
	{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		caller.set(tokenizer.nextToken());
		int calls = (new Integer(tokenizer.nextToken())).intValue();			
		output.collect(new IntWritable( (calls * -1) ), caller);
	}
}
