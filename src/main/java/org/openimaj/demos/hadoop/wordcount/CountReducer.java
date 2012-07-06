package org.openimaj.demos.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	public CountReducer() {}
	
	@Override
	protected void reduce(Text word, Iterable<LongWritable> counts, Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws IOException ,InterruptedException {
		long total = 0;
		for (LongWritable longWritable : counts) {
			total += longWritable.get();
		}
		context.write(word, new LongWritable(total));
	};
}