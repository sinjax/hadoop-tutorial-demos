package org.openimaj.demos.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openimaj.text.nlp.TweetTokeniser;

public class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	static final LongWritable one = new LongWritable(1);
	public CountMapper() {}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws IOException ,InterruptedException {
		try{				
			TweetTokeniser tok = new TweetTokeniser(value.toString());
			for (String token : tok.getStringTokens()) {
				context.write(new Text(token.toLowerCase()),one);
			}
		} catch(Exception e){
			
		}
	};
}