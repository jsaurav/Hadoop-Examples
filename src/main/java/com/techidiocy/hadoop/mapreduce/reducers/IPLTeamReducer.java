/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author saurabh
 *
 */
public class IPLTeamReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text teamName,Iterable<Text> names,Context context) throws IOException, InterruptedException {
		//List<String> players = new ArrayList<String>();
		StringBuffer playerNames = new StringBuffer();
		for(Text playerName : names ) {
			playerNames.append(playerName.toString() + "|");
		}
		//Collections.sort(players);
		context.write(teamName, new Text(playerNames.toString()));
	}

}
