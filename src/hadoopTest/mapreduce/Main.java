package hadoopTest.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Runs the Hadoop job!
 * @author nikhilrane, andrescanabal
 *
 */
public class Main 
{
	public static void main(String[] args) throws Exception 
	{

		JobConf callers = new JobConf(Main.class);
		callers.setJobName("callers");

		callers.setOutputKeyClass(Text.class);
		callers.setOutputValueClass(IntWritable.class);

		callers.setMapperClass(MapCallers.class);
		callers.setCombinerClass(CombinerCallers.class);
		callers.setReducerClass(ReduceCallers.class);

		callers.setInputFormat(TextInputFormat.class);
		callers.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(callers, new Path(args[0]));
		FileOutputFormat.setOutputPath(callers, new Path(args[1]));

		JobClient.runJob(callers);


		JobConf sort = new JobConf(Main.class);
		sort.setJobName("sort");
		sort.setNumReduceTasks(1);

		sort.setOutputKeyClass(IntWritable.class);
		sort.setOutputValueClass(Text.class);

		sort.setMapperClass(MapSort.class);
		sort.setCombinerClass(CombinerSort.class);
		sort.setReducerClass(ReduceSort.class);

		sort.setInputFormat(TextInputFormat.class);
		sort.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(sort, new Path(args[1]));
		FileOutputFormat.setOutputPath(sort, new Path(args[2]));

		JobClient.runJob(sort);
	}
}
