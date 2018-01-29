package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;


public class SortReducer extends Reducer<SortPair, NullWritable, Text, Text> {
   
    public void reduce(SortPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // output the word and average
        
        Text K = key.getWord();
        Text V = new Text(Double.toString(key.getPR()));

        context.write(K, V);

    }
}