/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.reducers;

import java.io.IOException;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.techidiocy.hadoop.mapreduce.io.Item;

/**
 * @author saurabh
 *
 */
public class FlipkartCorrelationCoeffReducer extends Reducer<Text,Item,Text,LongWritable> {
	
	@Override
	public void reduce(Text key,Iterable<Item> values,Context context) throws IOException, InterruptedException {
		int counter =0;
		for(Item item : values) {
			counter++;
		}
		
		double[] priceArray   =   new double[counter];	
		double[] ratingArray  =   new double[counter];	
		
		counter =0;
		for(Item item : values) {
			priceArray[counter] =  item.getItemPrice().get();
			ratingArray[counter] = item.getRating().get();
			counter++;
		}
		
		PearsonsCorrelation pearsonCorrelation = new PearsonsCorrelation();
		LongWritable coefficient = new LongWritable((long) pearsonCorrelation.correlation(priceArray, ratingArray));
		
		context.write(key, coefficient);
	}

}
