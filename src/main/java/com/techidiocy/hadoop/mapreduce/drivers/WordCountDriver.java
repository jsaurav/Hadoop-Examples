/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.techidiocy.hadoop.mapreduce.mappers.WordCountMapper;
import com.techidiocy.hadoop.mapreduce.reducers.WordCountReducer;

/**
 * @author saurabh
 *
 */
public class WordCountDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(),"WordCount");
		job.setJarByClass(WordCountDriver.class);
		
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
		return job.waitForCompletion(true) ? 0: 1;
	}

	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WordCountDriver(), args);
	    System.exit(res);
	}
}
