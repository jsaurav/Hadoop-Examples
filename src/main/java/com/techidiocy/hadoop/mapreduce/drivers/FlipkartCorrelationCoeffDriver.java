/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.techidiocy.hadoop.mapreduce.io.Item;
import com.techidiocy.hadoop.mapreduce.mappers.FlipkartCorrelationCoeffMapper;
import com.techidiocy.hadoop.mapreduce.reducers.FlipkartCorrelationCoeffReducer;

/**
 * @author saurabh
 *
 */
public class FlipkartCorrelationCoeffDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());	
		job.setJarByClass(FlipkartCorrelationCoeffDriver.class);
		job.setJobName("Flipkart Correlation Coefficeint Driver");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FlipkartCorrelationCoeffMapper.class);
		job.setReducerClass(FlipkartCorrelationCoeffReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Item.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}
	
	public static void main(String args[]) throws Exception {
		int status = ToolRunner.run(new FlipkartCorrelationCoeffDriver(), args);
		System.exit(status);
	}

}
