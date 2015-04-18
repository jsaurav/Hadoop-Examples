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

import com.techidiocy.hadoop.mapreduce.mappers.IPLTeamMapper;
import com.techidiocy.hadoop.mapreduce.reducers.IPLTeamReducer;

/**
 * @author saurabh
 *
 */
public class IPLTeamDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf());
		//getConf().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
		job.setJobName("IPL Team Finder");
		job.setJarByClass(IPLTeamDriver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(IPLTeamMapper.class);
		job.setReducerClass(IPLTeamReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		
		return job.waitForCompletion(true) ? 1:0;
	}

	public static void main(String args[]) throws Exception {
		int exitcode = ToolRunner.run(new IPLTeamDriver(), args);
		System.exit(exitcode);
	}
}
