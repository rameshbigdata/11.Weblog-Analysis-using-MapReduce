package com.ramesh.weblog;
 

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class LogPreprocessor
{
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		
		args = new String[] { 
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/8.WebLogAnalsyis/Input_data/weblogs_10_thousand_rec.txt",
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/8.WebLogAnalsyis/output_data/"};
				 
				/* delete the output directory before running the job */
				FileUtils.deleteDirectory(new File(args[1])); 
				 
				if (args.length != 2) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
				}
				
				System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
		
				
				
		Configuration conf = new Configuration();
	 
		Job job = new Job(conf, "Log Preprocessor");
		job.setJobName("Log Preprocessor");
		job.setJarByClass(LogPreprocessor.class);
		job.setMapperClass(LogPrepMapper.class);
		job.setReducerClass(LogPrepReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LogOutputWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
//		 MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, Text.class);
		
		MultipleOutputs.addNamedOutput(job, "ParsedRecords", TextOutputFormat.class , NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "BadRecords", TextOutputFormat.class , NullWritable.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}