package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class DanglingNode{
	
	public DanglingNode(){

  	}
	
	public void DanglingNode(String input_path, String output_path) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "DanglingNode");
		job.setJarByClass(DanglingNode.class);

		// set the inputFormatClass <K, V>
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(DanglingNodeMapper.class);
		job.setReducerClass(DanglingNodeReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(1);
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);

		PageRank.danglingNodeLong = job.getCounters()
				.findCounter(DanglingNodeReducer.UpdateCounter.SUMOFDANGLINGNODES)
				.getValue();

		PageRank.danglingNodeDouble = (double)PageRank.danglingNodeLong / 1000000000000000000L / PageRank.N;

		PageRank.numOfDanglingNode = job.getCounters()
				.findCounter(DanglingNodeReducer.UpdateCounter.DANGLINGNODECOUNT)
				.getValue();

	}
	
}
