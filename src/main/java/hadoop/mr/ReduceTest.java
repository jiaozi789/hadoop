package hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 使用远程的hdfs和mr发布
 * @author jiaozi
 *
 */
public class ReduceTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		// 设置操作使用的用户 如果不设置为root 和 hadoop服务的相同 出现异常 本机账号是window账号
		System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.job.jar", "target\\hadoop-0.0.1-SNAPSHOT.jar");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("/user/root/output")))
			fs.delete(new Path("/user/root/output"),true);
		Job job =Job.getInstance(conf, "WordCount");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReduce.class);
		FileInputFormat.addInputPath(job,new Path("/user/root/input"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output"));
		job.waitForCompletion(true);
	}
	public static class MyMapper extends Mapper<LongWritable,Text, Text, IntWritable>{
    	public MyMapper() {
		}
    	@Override
    	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
    			throws IOException, InterruptedException {
    		String[] split=value.toString().split(" ");
    		for(String s:split){
    			context.write(new Text(s),new IntWritable(1));
    		}
    	}
    }
    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    	public MyReduce() {
		}
    	@Override
    	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
    			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
    		Iterator<IntWritable> ite=arg1.iterator();
    		int i=0;
    		while(ite.hasNext()){
    			i++;
    			ite.next();
    		}
    		arg2.write(arg0, new IntWritable(i));
    	}
    }
}
