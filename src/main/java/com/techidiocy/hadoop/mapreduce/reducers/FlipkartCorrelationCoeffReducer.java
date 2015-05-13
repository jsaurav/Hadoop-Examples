/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.techidiocy.hadoop.mapreduce.io.Item;

/**
 * @author saurabh
 *
 */
public class FlipkartCorrelationCoeffReducer extends Reducer<Text,Item,Text,DoubleWritable> {
	
	@Override
	public void reduce(Text key,Iterable<Item> values,Context context) throws IOException, InterruptedException {
		List<Double> prices = new ArrayList<Double>();
		List<Double> ratings = new ArrayList<Double>();
		
		Iterator<Item> iterator = values.iterator();
		while(iterator.hasNext()) {
			Item item = (Item)iterator.next();
			prices.add((double) item.getItemPrice().get());
			ratings.add((double) item.getRating().get());
		}
		
		double[] priceArray   =   ArrayUtils.toPrimitive(prices.toArray(new Double[0]));	
		double[] ratingArray  =   ArrayUtils.toPrimitive(ratings.toArray(new Double[0]));
		
		
		PearsonsCorrelation pearsonCorrelation = new PearsonsCorrelation();
		double coeff = pearsonCorrelation.correlation(priceArray, ratingArray);
		DoubleWritable coefficient = new DoubleWritable(coeff);
		context.write(key, coefficient);
	}

}
