/**
 * 
 */
package com.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lyl
 *
 */
public class SecondarySortReducer extends Reducer<CustomWritable, IntWritable, Text, IntWritable>{
	private Text outputKey = new Text();
	@Override
	protected void reduce(CustomWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for(IntWritable value : values) {
			outputKey.set(key.getFirst());
			context.write(outputKey, value);
		}
	}
}
