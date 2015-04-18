/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author saurabh
 *
 */
public class IPLTeamMapper extends Mapper<Text,Text,Text,Text>{
	
	public void map(Text key,Text value,Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
