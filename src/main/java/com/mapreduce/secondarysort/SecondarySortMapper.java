/**
 * 
 */
package com.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lyl
 *
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CustomWritable, IntWritable> {

	private CustomWritable outputKey = new CustomWritable();
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if(StringUtils.isBlank(line))
			return;
		String[] fields = line.split(",");
		if (fields.length != 2)
			return;

		String oriKey = fields[0];
		int oriVal = Integer.valueOf(fields[1]);

		outputKey.set(oriKey, oriVal);
		outputValue.set(oriVal);

		context.write(outputKey, outputValue);
	}

}
