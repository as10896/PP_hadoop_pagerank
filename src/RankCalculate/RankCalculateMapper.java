package pageRank;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.net.URI; 
import java.io.*;


public class RankCalculateMapper extends Mapper<Text, Text, Text, Text> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	
		String v = value.toString();
		int rankTabIndex = v.indexOf("\t");
		Double PR = Double.parseDouble(v.substring(0, rankTabIndex));


		StringBuffer graph_structure = new StringBuffer("|");

		StringBuffer original_PR = new StringBuffer("@");
		original_PR.append(Double.toString(PR));

		// if page has outgoing links
		if(v.length() > (rankTabIndex + 1)){
			String links_non_split = v.substring(rankTabIndex+1, v.length());
			graph_structure.append(links_non_split);
			String[] links = links_non_split.split("@-c@=v@-k6z@h-n@");
			int out_degree = links.length;
			StringBuffer pr_output = new StringBuffer("p");
			pr_output.append(Double.toString(PR/out_degree));

			for(String link : links){
				context.write(new Text(link), new Text(pr_output.toString()));
			}
		}

		context.write(key, new Text(graph_structure.toString()));	// pass graph structure
		context.write(key, new Text(original_PR.toString()));		// pass original PR
	}

}
