package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;


public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {    

    // private static final Log LOG = LogFactory.getLog(RankCaculateReducer.class);

    public static enum ConvergenceCounter {
        SUMOFERROR
    }

    private static final double damping = 0.85;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        double sum_of_p = 0;
        String graph_structure = "";
        double original_PR = 0;

        for(Text val : values){
            String v = val.toString();            

            if(v.startsWith("p")){
                sum_of_p += Double.parseDouble(v.substring(1, v.length()));
            }
            else if(v.startsWith("|")){
                if(v.length() > 1)
                    graph_structure += v.substring(1, v.length());
            }
            else if(v.startsWith("@")){
                original_PR = Double.parseDouble(v.substring(1, v.length()));
            }
        }

        String confVariable = context.getConfiguration().get("danglingPR");
        Double danglingNodePR = Double.parseDouble(confVariable);
        // double danglingNodePR = context.getConfiguration().getDouble("danglingPR", 0);

        int N = context.getConfiguration().getInt("N", 30727);

        double new_PR = (1 - damping) * ((double)1/(double)N) + damping * (sum_of_p + danglingNodePR);
        
        StringBuffer output_value = new StringBuffer(Double.toString(new_PR));
        output_value.append("\t");
        output_value.append(graph_structure);

        context.write(key, new Text(output_value.toString()));

        double err = Math.abs(new_PR - original_PR);
        long counter_value = (long) (err * 1000000000000000000L);

        context.getCounter(ConvergenceCounter.SUMOFERROR).increment(counter_value);

        
        // LOG.info("\n\n\n\n"+ danglingNodePR.toString() +"\n\n\n\n");

	}
}
