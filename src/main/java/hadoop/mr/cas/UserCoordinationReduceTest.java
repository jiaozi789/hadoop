package hadoop.mr.cas;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.mr.cas.FofReduceTest.MyMapper;
import hadoop.mr.cas.FofReduceTest.MyMapper1;
import hadoop.mr.cas.FofReduceTest.MyReduce;
import hadoop.mr.cas.FofReduceTest.MyReduce1;
import hadoop.mr.cas.TempoReduceTest.Tempo;

/**
 * 基于用户的协同过滤推荐 (原理参考https://blog.csdn.net/liaomin416100569/article/details/79930701)
 *  某个用户A喜欢了某些物品  另外一个用户B也喜欢这些物品+其他物品  其他物品 被推荐给A
 * 
 * 模拟数据
 * A,物品A
 * A,物品C
 * B,物品B
 * C,物品A
 * C,物品C
 * C,物品D 
 * reduce1 得出结果
 *    A 物品A,物品C 
 *    B 物品B
 *    C 物品A,物品C,物品D
 * map2
 *   物品A,物品C_A 物品A,物品C
 *   物品B_B 物品B
 *   物品A,物品C,物品D_C 物品A,物品C,物品D
 *  两个键是包含关系的 分为一组 
 *   --------------------
 *   物品A,物品C_A 物品A,物品C
 *   物品A,物品C,物品D_C 物品A,物品C,物品D
 *   ----------------
 *   物品B_B 物品B
 *  
 * reduce2 模拟  
 *    A 物品D
 * 
 * @author jiaozi
 *
 */
public class UserCoordinationReduceTest {
	public static void main(String[] args) throws Exception {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("c:/user/ucr_output")))
			fs.delete(new Path("c:/user/ucr_output"), true);
		Job job = Job.getInstance(conf, "ucr_Calc");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReduce.class);
		FileInputFormat.addInputPath(job, new Path("c:/user/ucr_input"));
		FileOutputFormat.setOutputPath(job, new Path("c:/user/ucr_output"));
		if(job.waitForCompletion(true)){
			Configuration conf1 = new Configuration();
			FileSystem fs1 = FileSystem.get(conf);
			if (fs1.exists(new Path("c:/user/ucr_output1")))
				fs1.delete(new Path("c:/user/ucr_output1"), true);
			Job job1 = Job.getInstance(conf, "Fof_Calc");
			job1.setMapperClass(MyMapper1.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setGroupingComparatorClass(GroupCompartor.class);
			job1.setReducerClass(MyReduce1.class);
			FileInputFormat.addInputPath(job1, new Path("c:/user/ucr_output"));
			FileOutputFormat.setOutputPath(job1, new Path("c:/user/ucr_output1"));
			job1.waitForCompletion(true);
		}
	}
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text>{
    	public MyMapper() {
		}
    	@Override
    	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    			throws IOException, InterruptedException {
    		String[] s=value.toString().split(",");
    		String f1=s[0];
    		String f2=s[1];
    		context.write(new Text(f1),new Text(f2));
    	}
    }
	
    public static class MyReduce extends Reducer<Text, Text, Text, Text>{
    	public MyReduce() {
    		
		}
    	@Override
    	protected void reduce(Text key, Iterable<Text> arg1,
    			Reducer<Text, Text, Text, Text>.Context c) throws IOException, InterruptedException {
    		Iterator<Text> iterator = arg1.iterator(); 
    		StringBuffer sb=new StringBuffer();
    		while(iterator.hasNext()){
    			Text next = iterator.next();
    			sb.append(next.toString()+",");
    		}
    		if(sb.toString().endsWith(",")){
    			c.write(key, new Text(sb.substring(0, sb.length()-1)));
    		}
    	}   	
    }
    /**
     * 
     * A 物品A,物品C 
 *    B 物品B
 *    C 物品A,物品C,物品D
     * 转换成
	 * 物品A,物品C_A 物品A,物品C
 *   物品B_B 物品B
 *   物品A,物品C,物品D_C 物品A,物品C,物品D
	 * @author jiaozi
	 *
	 */
    public static class MyMapper1 extends Mapper<LongWritable,Text, Text, Text>{
    	public MyMapper1() {
		}
    	@Override
    	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    			throws IOException, InterruptedException {
    		String[] s=value.toString().split("\t");
    		String f1=s[0];
    		String f2=s[1];
    		context.write(new Text(f2+"_"+f1),new Text(f2));
    	}
    }
    public static class MyReduce1 extends Reducer<Text, Text, Text, Text>{
    	public MyReduce1() {
		}
    	@Override
    	protected void reduce(Text key, Iterable<Text> arg1,
    			Reducer<Text, Text, Text, Text>.Context c) throws IOException, InterruptedException {
    		Iterator<Text> iterator = arg1.iterator(); 
    		System.out.println("-----------------------");
    		while(iterator.hasNext()){
    			Text next = iterator.next();
    			System.out.println(next.toString());
    		}
    	}
    }
    public static class GroupCompartor extends WritableComparator{
		public GroupCompartor() {
			super(Text.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			try {
				//物品A,物品C_A 或者  物品A,物品C,物品D_C 只要有包含关系分为一组
				Text textA=(Text)a;
				Text textB=(Text)b;
				//去掉_后用户部分
				String[] sp1=textA.toString().split("_")[0].split(",");
				String[] sp2=textB.toString().split("_")[0].split(",");
				String[] max=sp2.length>=sp1.length?sp2:sp1;
				String[] min=sp2.length>=sp1.length?sp1:sp2;
				int equalLength=0;
				for(String minTemp:min){
					for(String maxTemp:max){
						if(minTemp.equals(maxTemp)){
							equalLength++;
						}
					}
				}
				return equalLength-min.length;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return -1;
		}
	}
}
