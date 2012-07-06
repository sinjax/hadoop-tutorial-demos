package org.openimaj.demos.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountSorterMap extends Mapper<LongWritable,Text,LongWritable,Text>{
	public CountSorterMap(){}
	protected void map(LongWritable line, Text text, Mapper<LongWritable,Text,LongWritable,Text>.Context context) throws IOException ,InterruptedException {
		String[] wordcount = text.toString().split("\t");
		context.write(new LongWritable(Long.parseLong(wordcount[1])), new Text(wordcount[0]));
	}
}