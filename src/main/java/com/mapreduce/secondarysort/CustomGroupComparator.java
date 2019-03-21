/**
 * 
 */
package com.mapreduce.secondarysort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lyl
 * 自定义分组   跟map输出的key的类型有关
 */
public class CustomGroupComparator implements RawComparator<CustomWritable>{

	@Override
	public int compare(CustomWritable o1, CustomWritable o2) {
		// 比较两个map输出的key值的原始key	
		return o1.getFirst().compareTo(o2.getFirst());
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// byte[] b1, int s1, int l1 first + second(int) = l1
		// byte[] b2, int s2, int l2
		// first在字节数据byte里的起始位置是0，总长度是l1，int类型是4个字节，l1 - 4就是first字节长度
		return WritableComparator.compareBytes(b1, 0, l1 - 4, b2, 0, l2 - 4);
	}

}
