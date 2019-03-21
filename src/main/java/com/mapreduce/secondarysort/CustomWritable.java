/**
 * 
 */
package com.mapreduce.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author lyl
 * 自定义map输出key的类型
 */
public class CustomWritable implements WritableComparable<CustomWritable> {
	private String first;	// 原始key值
	private int second;		// 原始value值
	
	
	public CustomWritable() {
		
	}
	
	public CustomWritable(String firstVal,int secondVal) {
		this.set(firstVal, secondVal);
	}
	
	public void set(String first,int second) {
		this.first=first;
		this.second=second;
	}
	
	/* (non-Javadoc)
	 * 序列化
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeInt(second);
	}
	/* (non-Javadoc)
	 * 反序列化
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		 this.first=in.readUTF();
		 this.second=in.readInt();
	}
	/* (non-Javadoc)
	 * 重新排序方法，支持二次排序
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(CustomWritable other) {
		// 先比较原始的key值，如果key相等，在比较原始的value
		int result = this.first.compareTo(other.getFirst());
		// 如果原始key不等，返回
		if (result != 0)
			return result;
		return Integer.valueOf(this.second).compareTo(Integer.valueOf(other.getSecond()));
	}
	
	public String getFirst() {
		return first;
	}
	public void setFirst(String first) {
		this.first = first;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}

}
