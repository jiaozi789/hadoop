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
 * 使用window本地的hadoop来进行mr测试  可以调试 
 * @author jiaozi
 *
 */
public class LocalReduceTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("c:/user/output")))
			fs.delete(new Path("c:/user/output"),true);
		Job job =Job.getInstance(conf, "WordCount");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReduce.class);
		FileInputFormat.addInputPath(job,new Path("c:/user/input"));
		FileOutputFormat.setOutputPath(job, new Path("c:/user/output"));
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
