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
 * �����û���Эͬ�����Ƽ� (ԭ��ο�https://blog.csdn.net/liaomin416100569/article/details/79930701)
 *  ĳ���û�Aϲ����ĳЩ��Ʒ  ����һ���û�BҲϲ����Щ��Ʒ+������Ʒ  ������Ʒ ���Ƽ���A
 * 
 * ģ������
 * A,��ƷA
 * A,��ƷC
 * B,��ƷB
 * C,��ƷA
 * C,��ƷC
 * C,��ƷD 
 * reduce1 �ó����
 *    A ��ƷA,��ƷC 
 *    B ��ƷB
 *    C ��ƷA,��ƷC,��ƷD
 * map2
 *   ��ƷA,��ƷC_A ��ƷA,��ƷC
 *   ��ƷB_B ��ƷB
 *   ��ƷA,��ƷC,��ƷD_C ��ƷA,��ƷC,��ƷD
 *  �������ǰ�����ϵ�� ��Ϊһ�� 
 *   --------------------
 *   ��ƷA,��ƷC_A ��ƷA,��ƷC
 *   ��ƷA,��ƷC,��ƷD_C ��ƷA,��ƷC,��ƷD
 *   ----------------
 *   ��ƷB_B ��ƷB
 *  
 * reduce2 ģ��  
 *    A ��ƷD
 * 
 * @author jiaozi
 *
 */
public class UserCoordinationReduceTest {
	public static void main(String[] args) throws Exception {
		// ���õ�ǰ������hadoopĿ¼
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
     * A ��ƷA,��ƷC 
 *    B ��ƷB
 *    C ��ƷA,��ƷC,��ƷD
     * ת����
	 * ��ƷA,��ƷC_A ��ƷA,��ƷC
 *   ��ƷB_B ��ƷB
 *   ��ƷA,��ƷC,��ƷD_C ��ƷA,��ƷC,��ƷD
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
				//��ƷA,��ƷC_A ����  ��ƷA,��ƷC,��ƷD_C ֻҪ�а�����ϵ��Ϊһ��
				Text textA=(Text)a;
				Text textB=(Text)b;
				//ȥ��_���û�����
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
