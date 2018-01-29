package pageRank;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pageRank.DanglingNode;

public class PageRank{

	public static Long danglingNodeLong;
	public static Double danglingNodeDouble;
	public static Long convergenceCheckerLong;
	public static Double convergenceCheckerDouble;
	public static Long numOfDanglingNode;
	public static int N;

	private static Map<Integer, Double> ErrorList = new HashMap<>();

	private static final String ReducerNum = "32";

	private static NumberFormat nf = new DecimalFormat("0000");
	
    public static void main(String[] args) throws Exception {
		
		long startTime = System.nanoTime();;

		ParseAndRemoveInvalidLink job1 = new ParseAndRemoveInvalidLink();
		job1.ParseAndRemoveInvalidLink(args[0], "PageRank/RemoveInvalidLink", ReducerNum);

		RecoverParseOutput job2 = new RecoverParseOutput();
		job2.RecoverParseOutput("PageRank/RemoveInvalidLink", "PageRank/iter/0000", ReducerNum);
		
		int iteration = 0;
		
		if(args.length == 3){
			int iterNum = Integer.valueOf(args[2]);

			for(int i = 0; i < iterNum; ++i){
				String inputPath_j3_j4 = "PageRank/iter/" + nf.format(iteration);
				DanglingNode job3 = new DanglingNode();
				job3.DanglingNode(inputPath_j3_j4, "PageRank/dangling/" + nf.format(iteration));
		
				iteration++;
	
				String outputPath_j4 = "PageRank/iter/" + nf.format(iteration);
				RankCalculate job4 = new RankCalculate();
				job4.RankCalculate(inputPath_j3_j4, outputPath_j4, ReducerNum);

				ErrorList.put(iteration, convergenceCheckerDouble);
				System.out.printf("iter: %d\nerror: %s\n", iteration, convergenceCheckerDouble.toString());	
			}
		}
		else{
			do{
				String inputPath_j3_j4 = "PageRank/iter/" + nf.format(iteration);
				DanglingNode job3 = new DanglingNode();
				job3.DanglingNode(inputPath_j3_j4, "PageRank/dangling/" + nf.format(iteration));
		
				iteration++;
	
				String outputPath_j4 = "PageRank/iter/" + nf.format(iteration);
				RankCalculate job4 = new RankCalculate();
				job4.RankCalculate(inputPath_j3_j4, outputPath_j4, ReducerNum);
	
				ErrorList.put(iteration, convergenceCheckerDouble);
				System.out.printf("iter: %d\nerror: %s\n", iteration, convergenceCheckerDouble.toString());	
	
			} while(convergenceCheckerDouble >= 0.001);
		}


		String final_inputPath = "PageRank/iter/" + nf.format(iteration);

		Sort job5 = new Sort();
		job5.Sort(final_inputPath, args[1]);

		long endTime = System.nanoTime();
		long total = endTime - startTime;
		
		System.out.printf("Number of page: %d\n", N);
		System.out.printf("Number of dangling node: %s\n", numOfDanglingNode.toString());
		System.out.printf("Iteration: %d\n", iteration);
		System.out.printf("Total execution time: %d sec\n", TimeUnit.NANOSECONDS.toSeconds(total));

		System.out.println("\nConvergence of PageRank:");
		System.out.println("\niteration\terror");
		for(Map.Entry e : ErrorList.entrySet()){
			System.out.println(e.getKey()+"\t\t"+e.getValue());
		}
		
		System.exit(0);
	}  
}
