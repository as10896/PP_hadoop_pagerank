package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Sort {
	
	public Sort(){

    }
	
	public void Sort(String input_path, String output_path) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Sort");
		job.setJarByClass(Sort.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(SortPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(1);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));

		job.waitForCompletion(true);
	}
}
