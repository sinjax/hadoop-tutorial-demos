package org.openimaj.demos.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountSorterReduce extends Reducer<LongWritable,Text,Text,LongWritable>{
	public CountSorterReduce(){};
	protected void reduce(LongWritable count, Iterable<Text> words, Reducer<LongWritable,Text,Text,LongWritable>.Context context) throws IOException ,InterruptedException {
		for (Text word : words) {
			context.write(word, count);
		}
	};
}