/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.techidiocy.hadoop.mapreduce.mappers.FacebookMutualFriendsMapper;
import com.techidiocy.hadoop.mapreduce.reducers.FacebookMutualFriendsReducer;

/**
 * @author saurabh
 *
 */
public class FacebookMutualFriendsDriver extends Configured implements Tool	 {

	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf());
		job.setJobName("Facebook Mutual Friends");
		job.setJarByClass(FacebookMutualFriendsDriver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FacebookMutualFriendsMapper.class);
		job.setReducerClass(FacebookMutualFriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		return job.waitForCompletion(true) ? 0:1;
	}
	
	public static void main(String args[]) throws Exception {
		int status = ToolRunner.run(new FacebookMutualFriendsDriver(), args);
		System.exit(status);
	}

}
