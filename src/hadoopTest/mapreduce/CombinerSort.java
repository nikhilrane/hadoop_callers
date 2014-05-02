package hadoopTest.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CombinerSort extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> 
{
	private Text caller = new Text();
	
	//(sum, Coller), Ex (10, "A") => (Coller, sum), Ex ("A", 10)
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException 
	{
		while (values.hasNext()) 
		{
			caller.set(values.next());
			output.collect(key, caller);
		}
	}
  }
