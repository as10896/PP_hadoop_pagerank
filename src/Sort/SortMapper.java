package pageRank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;


public class SortMapper extends Mapper<Text, Text, SortPair, NullWritable> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        
        String v = value.toString();
		int rankTabIndex = v.indexOf("\t");
		double PR = Double.parseDouble(v.substring(0, rankTabIndex));
        
		SortPair K = new SortPair(key, PR);

		context.write(K, NullWritable.get());
	}
	
}
