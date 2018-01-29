package pageRank;
import java.io.IOException;
import java.util.StringTokenizer;

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


public class RecoverParseOutputMapper extends Mapper<Text, Text, Text, Text> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
     
        context.write(key, new Text("!!!!!!!"));


        if(value.toString().isEmpty()) return;

        String[] pages = value.toString().split("@-c@=v@-k6z@h-n@");

        for(String page : pages){
            context.write(new Text(page), key);
        }
    }
}
