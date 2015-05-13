/**
 * 
 */
package com.techidiocy.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author saurabh
 *
 */
public class Item implements WritableComparable<Item> {
	
	private LongWritable itemPrice;
	private IntWritable rating;
	
	public Item() {
		
	}
	
	public Item(LongWritable itemPrice , IntWritable rating) {
		this.itemPrice= itemPrice;
		this.rating = rating;
	}
	
	public LongWritable getItemPrice() {
		return itemPrice;
	}

	public void setItemPrice(LongWritable itemPrice) {
		this.itemPrice = itemPrice;
	}
	
	public IntWritable getRating() {
		return rating;
	}

	public void setRating(IntWritable rating) {
		this.rating = rating;
	}

	public void write(DataOutput out) throws IOException {
		out.writeBytes(itemPrice.toString() +"," + rating.toString());
	}

	public void readFields(DataInput in) throws IOException {
		String data = in.readLine();
		itemPrice = new LongWritable(Long.parseLong(data.substring(0,data.indexOf(","))));
		rating = new IntWritable(Integer.parseInt(data.substring(data.indexOf(",")+1)));
	}

	public int compareTo(Item o) {		
		return this.itemPrice.compareTo(o.itemPrice);
	}

}
