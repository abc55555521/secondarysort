/**
 * 
 */
package com.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lyl
 * 泛型是map输出的k-v对的类型
 * 调整分区,map输出的key类型有修改，会导致相同的key不能分到同一分区 
 */
public class CustomPartitioner extends Partitioner<CustomWritable,IntWritable> {

	@Override
	public int getPartition(CustomWritable key, IntWritable value, int numPartitions) {
		// numPartitions： reduce task的数量
		// &Integer.MAX_VALUE是为了防止 hashCode是负数
		return key.getFirst().hashCode() & Integer.MAX_VALUE % numPartitions;
	}

}
