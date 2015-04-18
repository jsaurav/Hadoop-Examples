/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.techidiocy.hadoop.mapreduce.constants.Constants;

/**
 * @author saurabh
 *
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	private final IntWritable ONE = new IntWritable(1); 
	private Text word = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString(),Constants.WHITE_SPACE);
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, ONE);
		}
	}

}
