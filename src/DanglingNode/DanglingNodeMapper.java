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



public class DanglingNodeMapper extends Mapper<Text, Text, Text, DoubleWritable> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        
		String v = value.toString();
		int rankTabIndex = v.indexOf("\t");
		
		DoubleWritable PR_dangling_node;
		
		if(v.length() == (rankTabIndex + 1)){
			PR_dangling_node = new DoubleWritable(Double.parseDouble(v.substring(0, rankTabIndex)));
		}
		else{
			return;
		}
		
		Text K = new Text("sum");
		context.write(K, PR_dangling_node);

	}

}
