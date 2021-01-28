package cn.layfolk._12compress;

import cn.layfolk._01wordcount.WordcountMapper;
import cn.layfolk._01wordcount.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RedWordcountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 1 获取配置信息以及封装任务
		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration);

		// 2 设置jar加载路径
		job.setJarByClass(RedWordcountDriver.class);

		// 3 设置map和reduce类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		// 4 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置reduce端输出压缩开启
		FileOutputFormat.setCompressOutput(job, true);
		// 设置压缩的方式
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path("d:/input/hello.txt"));
		FileOutputFormat.setOutputPath(job, new Path("d:/output"));

		// 7 提交
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
