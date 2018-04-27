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
 * ʹ��Զ�̵�hdfs��mr����
 * @author jiaozi
 *
 */
public class ReduceTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// ���õ�ǰ������hadoopĿ¼
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		// ���ò���ʹ�õ��û� ���������Ϊroot �� hadoop�������ͬ �����쳣 �����˺���window�˺�
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
