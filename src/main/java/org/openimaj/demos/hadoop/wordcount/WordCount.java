package org.openimaj.demos.hadoop.wordcount;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool{
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2 )return 1;
		String in = args[0];
		String out = args[1];
		
		// COUNT THE WORDS
		
		Job job = new Job(this.getConf());
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(CountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setReducerClass(CountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		TextInputFormat.setInputPaths(job, new Path(in));
		TextOutputFormat.setOutputPath(job, new Path(out));
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		
		// SORT THE COUNTED WORDS
		
		job = new Job(this.getConf());
		job.setJarByClass(WordCount.class);
		job.setMapperClass(CountSorterMap.class);
		job.setReducerClass(CountSorterReduce.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		TextInputFormat.setInputPaths(job, new Path(out));
		TextOutputFormat.setOutputPath(job, new Path(out + "_sort"));
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WordCount(), args);
	}
	
}
