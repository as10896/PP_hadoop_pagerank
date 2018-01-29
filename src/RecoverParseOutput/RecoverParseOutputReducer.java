package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecoverParseOutputReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int N = context.getConfiguration().getInt("N", 30727);

        double PR_init = (double) 1/(double) N;
        StringBuffer output_value = new StringBuffer();
        StringBuffer links_buffer = new StringBuffer();

        boolean first = true;

        for(Text val : values){
            String v = val.toString();

            if(v.startsWith("!!!!!!!")){
                output_value.append(Double.toString(PR_init));
                output_value.append("\t");
            }
            else{
                if(!first) links_buffer.append("@-c@=v@-k6z@h-n@");
                links_buffer.append(v);
                first = false;
            }
        }

        output_value.append(links_buffer.toString());
        context.write(key, new Text(output_value.toString()));
	}
}
