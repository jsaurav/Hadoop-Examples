/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.reducers;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.techidiocy.hadoop.mapreduce.constants.Constants;

/**
 * @author saurabh
 *
 */
public class FacebookMutualFriendsReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		List<String> mutualFriendsPair = new LinkedList<String>();
		for(Text value : values) {
			mutualFriendsPair.add(value.toString());
		}
		String mutualFriends = getMutualFriends(mutualFriendsPair);
		context.write(key, new Text(mutualFriends));
	}

	private String getMutualFriends(List<String> mutualFriendsPair) {
		Map<String, Integer> nameMap = new TreeMap<String, Integer>();
		for (String friendList : mutualFriendsPair) {
			StringTokenizer tokenizer = new StringTokenizer(friendList,Constants.PIPE);
			while(tokenizer.hasMoreElements()) {
				String friend = tokenizer.nextToken();
				if (nameMap.containsKey(friend)) {
					nameMap.put(friend, nameMap.get(friend) + 1);
				} else {
					nameMap.put(friend, 1);
				}
			}
		}

		String mutualFriends = "";
		for (Entry<String, Integer> entry : nameMap.entrySet()) {
			if (entry.getValue().equals(mutualFriendsPair.size())) {
				mutualFriends = mutualFriends + entry.getKey() + Constants.PIPE;
			}
		}
		return mutualFriends.length() > 0 ? mutualFriends.substring(0,
				mutualFriends.lastIndexOf(Constants.PIPE))
				: "No Mutual Friends";
	}
}
