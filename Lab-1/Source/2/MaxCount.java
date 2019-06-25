package com.code;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class MaxCount extends Configured implements Tool{
    public static class MaxStockPriceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
            
        private final static IntWritable one = new IntWritable(1);
        private Text symbol = new Text();
        public void map(LongWritable key, Text value, Context context) 
                 throws IOException, InterruptedException {
             String[] stringArr = value.toString().split("\t");
             symbol.set(stringArr[0]);
             Integer price = Integer.parseInt(stringArr[1]);
             context.write(symbol, new IntWritable(price));
         }
    }
    public static class MaxStockPriceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
 
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int    maxValue = Integer.MIN_VALUE;
            for (IntWritable val : values) {
                maxValue = Math.max(maxValue, val.get());
            }      
            context.write(key, new IntWritable(maxValue));
        }
    }
    
    public static void main(String[] args) throws Exception {
        int exitFlag = ToolRunner.run(new MaxCount(), args);
        System.exit(exitFlag);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock price");
        job.setJarByClass(getClass());
        job.setMapperClass(MaxStockPriceMapper.class);    
        job.setCombinerClass(MaxStockPriceReducer.class);
        job.setReducerClass(MaxStockPriceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
