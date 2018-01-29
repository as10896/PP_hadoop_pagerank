package pageRank;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
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



public class ParseAndRemoveInvalidLinkMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
         	
		/*  Match title pattern */  
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(value.toString());
		// No need capitalizeFirstLetter
		titleMatcher.find();
		Text title = new Text(unescapeXML(parseTitle(titleMatcher.group())));

		context.write(title, new Text("!!!!!!!"));
		
		/*  Match link pattern */
		Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher(value.toString());
		// Need capitalizeFirstLetter

		while(linkMatcher.find()){
			String s = capitalizeFirstLetter(unescapeXML(parseLink(linkMatcher.group())));
			if(s.isEmpty()) continue;

			context.write(new Text(s), title);
		}
	}
	
	private String unescapeXML(String input) {

		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");

    }

    private String capitalizeFirstLetter(String input){

		if(input.isEmpty()) return "";

    	char firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else
                return input.substring(0, 1).toUpperCase() + input.substring(1);
        }
        else 
        	return input;
	}	

	private String parseTitle(String input){
		int start = 7;
		int end = input.indexOf("</title>", start);
		return input.substring(start, end);
	}

	private String parseLink(String input){
		int start = 2;
		int end = input.indexOf("]]", start);
		int sharp_index = input.indexOf("#", start);
		int pipe_index = input.indexOf("|", start);

		if(sharp_index > 0) end = sharp_index;
		if(pipe_index > 0) end = pipe_index;
		if(sharp_index > 0 && pipe_index > 0) end = (sharp_index < pipe_index) ? sharp_index : pipe_index;

		return input.substring(start, end);
	}

}
