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

public class MapCallers extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);
	private Text caller = new Text();

	// key, "Date,Caller,Callee", Ex "2012,A,B" =>  (Coller,1), Ex ("A",1)
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
	{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ",");			
		if (tokenizer.hasMoreTokens() && tokenizer.nextToken().equals("2012")) 
		{
			caller.set(tokenizer.nextToken());
			output.collect(caller, one);
		}
	}
}
