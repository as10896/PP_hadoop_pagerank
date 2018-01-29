package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseAndRemoveInvalidLinkReducer extends Reducer<Text, Text, Text, Text> {

    public static enum TotalPageCounter {
        TOTALPAGE
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer output_value = new StringBuffer();

        boolean first = true;

        boolean isExistingWikiPage = false;

        for(Text val : values){
            
            String v = val.toString();

            if(v.startsWith("!!!!!!!")){
                isExistingWikiPage = true;
            }
            else{
                if(!first) output_value.append("@-c@=v@-k6z@h-n@");
                output_value.append(v);
                first = false;
            }
        }

        if(!isExistingWikiPage) return;

        context.write(key, new Text(output_value.toString()));

        context.getCounter(TotalPageCounter.TOTALPAGE).increment(1);
	}
}
