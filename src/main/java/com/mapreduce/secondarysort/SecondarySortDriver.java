/**
 * 
 */
package com.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author lyl
 * 创建目录:hdfs dfs -mkdir -p /secondarysort/input
 * 删除:hadoop dfs -rm /secondarysort/input/sort.txt
 * 上传:hadoop dfs -put ~/sort.txt /secondarysort/input
 * 删除输出目录:hadoop dfs  -rm -r -f /secondarysort/output
 * 查看运行结果:hadoop dfs -cat /secondarysort/output/part*
 */
public class SecondarySortDriver {

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//本地调试用
		/*		args = new String[] {
				"hdfs://hadoopslave1:9000/secondarysort/input", 
				"hdfs://hadoopslave1:9000/secondarysort/output" 
				};*/
		// hadoop默认配置参数
		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://hadoopslave1:9000");

		// 获取job实例对象
		Job job = Job.getInstance(conf);
		job.setJarByClass(SecondarySortDriver.class);

		// 数据的输入路径
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 数据的输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 指定自定义分区和分组类
		job.setPartitionerClass(CustomPartitioner.class);
		job.setGroupingComparatorClass(CustomGroupComparator.class);

		// 指定使用的map和reduce方法所在类
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);

		// 指定map输出的k-v的类型
		job.setMapOutputKeyClass(CustomWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 指定reduce输出的k-v对的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 提交,result是true说明执行成功了
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
