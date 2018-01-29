package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class RankCalculate{
	
	public RankCalculate(){

  	}
	
	public void RankCalculate(String input_path, String output_path, String num_reducer) throws Exception {
		Configuration conf = new Configuration();
		// conf.setDouble("danglingPR", PageRank.danglingNodeDouble);
		conf.set("danglingPR", PageRank.danglingNodeDouble.toString());
		conf.setInt("N", PageRank.N);
		
		Job job = Job.getInstance(conf, "RankCalculate");
		job.setJarByClass(RankCalculate.class);

		// set the inputFormatClass <K, V>
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// set the class of each stage in mapreduce
		job.setMapperClass(RankCalculateMapper.class);
		job.setReducerClass(RankCalculateReducer.class);
		
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(Integer.valueOf(num_reducer));
		
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);
		
		// get sum of error (long type)
		PageRank.convergenceCheckerLong = job.getCounters()
				.findCounter(RankCalculateReducer.ConvergenceCounter.SUMOFERROR)
				.getValue();
		
		// convert back to double type
		PageRank.convergenceCheckerDouble = (double)PageRank.convergenceCheckerLong / 1000000000000000000L;


		// System.out.print("\n\n\n\nDPR:\t" + PageRank.danglingNodeDouble.toString() + "\n\n\n\n");
	}
	
}
