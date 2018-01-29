package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DanglingNodeReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    
    public static enum UpdateCounter {
        SUMOFDANGLINGNODES, DANGLINGNODECOUNT
    }

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0;
        int count = 0;
        
        for(DoubleWritable val : values){
            sum += val.get();
            count++;
        }

        long output_value = (long) (sum * 1000000000000000000L);

        context.getCounter(UpdateCounter.SUMOFDANGLINGNODES).setValue(output_value);
        context.getCounter(UpdateCounter.DANGLINGNODECOUNT).setValue((long)count);
	}
}
