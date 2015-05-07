/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.techidiocy.hadoop.mapreduce.constants.Constants;

/**
 * @author saurabh
 *
 */
public class FacebookMutualFriendsMapper extends Mapper<Text,Text,Text,Text> {

	
	public void map(Text key,Text value,Context context) throws IOException, InterruptedException {
		String friends = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(friends, Constants.PIPE);
		while(tokenizer.hasMoreElements()) {
			String token = tokenizer.nextToken();
			Text emmitedKey = new Text(arrange(key.toString(),token));
			context.write(emmitedKey, value);
		}
	}

	private String arrange(String key, String token) {
		if(key.compareToIgnoreCase(token) > 0)
			return token+"~"+key;
		else
			return key+"~"+token;
	}
	
}
