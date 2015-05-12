/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.techidiocy.hadoop.mapreduce.constants.Constants;
import com.techidiocy.hadoop.mapreduce.io.Item;

/**
 * @author saurabh
 *
 */
public class FlipkartCorrelationCoeffMapper extends Mapper<Text,Text,Text,Item> {
	
	public void map(Text key,Text value,Context context) throws IOException, InterruptedException {
		String productName = getProductName(key);
		String data = value.toString();
		String price = data.substring(0,data.indexOf(","));
		String rating = data.substring(data.indexOf(",")+1);
		context.write(new Text(productName), new Item(new LongWritable(new Long(price)),
				new IntWritable(new Integer(rating))));
	}

	private String getProductName(Text key) {
		String productName = key.toString();
		if(productName.contains(Constants.LG) && productName.contains(Constants.RFERIGERATOR))
			productName = Constants.LG_REFRIGERATOR;
		else if(productName.contains(Constants.IPHONE6) && productName.contains(Constants.APPLE))
			productName = Constants.APPLE_IPHONE6;
		else if(productName.contains(Constants.HITACHI) && productName.contains(Constants.AC))
			productName = Constants.HITACHI_AC;
		else if(productName.contains(Constants.D_LINK) && productName.contains(Constants.ROUTER))
			productName = Constants.D_LINK_ROUTER;
		else if(productName.contains(Constants.SAMSUNG) && productName.contains(Constants.TV))
			productName = Constants.SAMUSNG_TV;
		return productName;
	}
}
