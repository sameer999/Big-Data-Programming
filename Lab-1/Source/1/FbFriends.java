package com.code.fb;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FbFriends {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text pair = new Text();
		private Text List = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\t");
			String User = line[0];
			if (line.length ==2) {
				ArrayList<String> FriendsList = new ArrayList<String>(Arrays.asList(line[1].split("\\,")));
				for(String Friend:FriendsList){				
					String FriendPair = (Integer.parseInt(User) < Integer.parseInt(Friend))?User+"\t"+Friend:Friend+"\t"+User;
					ArrayList<String> temp = new ArrayList<String>(FriendsList);
					temp.remove(Friend);
					StringBuilder sb = new StringBuilder();
					for(String s: temp){
						sb.append(",").append(s);
					}
					String listString = sb.deleteCharAt(0).toString();
					pair.set(FriendPair);
					List.set(listString);
					context.write(pair,List);
				}
			}
			
		}
	}
		
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		public String Mutual(String s1,String s2,int i) {
			HashSet<String> map = new HashSet<String>();
			String[] s1_split = s1.split("\\,");
			String[] s2_split = s2.split("\\,");
			String result = "";
			for(String s:s1_split) {
				map.add(s);
			}
			for(String s:s2_split) {
				if (map.contains(s)){
					result +=s+",";
				}
			}
			return result;	
		}
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			String[] Friend_key_values = new String[2];
			int i=0;
			for(Text value:values){
				Friend_key_values[i++] = value.toString();
			}
			result.set(Mutual(Friend_key_values[0],Friend_key_values[1],i));
			i++;
			context.write(key,result);
		}
	}
		
	public static void main(String[] args) throws Exception {
		  	Configuration conf = new Configuration();
		  	String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		  	if (otherArgs.length != 2) {
		  	System.out.println(otherArgs[0]);
		  	System.err.println("Usage: FbFriends <in> <out>");
		  	System.exit(2);
		  	}
		  	Job job = new Job(conf, "FbFriends");
		  	job.setJarByClass(FbFriends.class);
		  	job.setMapperClass(Map.class);
		  	job.setReducerClass(Reduce.class);
		  	job.setOutputKeyClass(Text.class);
		  	job.setOutputValueClass(Text.class);
		  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		  	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	

}
