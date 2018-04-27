package hadoop.mr.cas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *参考文档Hadoop案例之二度人脉与好友推荐.docx
	以下表格数据表示 某个用户和某个用户是好友关系 推荐二级人脉 比如A和B是好友 推荐B的好友给A 
	A,B
	B,C
	C,D
	D,E
	E,F
	F,D
	F,C
	F,G
	G,I
	G,H
	H,I
	I,C
	H,A
 * @author jiaozi
 */
public class FofReduceTest {
	public static void main(String[] args) throws Exception {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("c:/user/fof_output")))
			fs.delete(new Path("c:/user/fof_output"), true);
		Job job = Job.getInstance(conf, "Fof_Calc");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReduce.class);
		FileInputFormat.addInputPath(job, new Path("c:/user/fof_input"));
		FileOutputFormat.setOutputPath(job, new Path("c:/user/fof_output"));
		if(job.waitForCompletion(true)){
			Configuration conf1 = new Configuration();
			FileSystem fs1 = FileSystem.get(conf);
			if (fs1.exists(new Path("c:/user/fof_output1")))
				fs1.delete(new Path("c:/user/fof_output1"), true);
			Job job1 = Job.getInstance(conf, "Fof_Calc");
			job1.setMapperClass(MyMapper1.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setReducerClass(MyReduce1.class);
			FileInputFormat.addInputPath(job1, new Path("c:/user/fof_output"));
			FileOutputFormat.setOutputPath(job1, new Path("c:/user/fof_output1"));
			job1.waitForCompletion(true);
		}
	}
	/*
	 * A,B
	 * B,C
	 * C,F
	 * 
	 * map会写入
	 * A,B_ori
	 * B,A_tmp
	 * B,C_ori
	 * C,B_tmp
	 * C,F_ori
	 * F,C_temp
	 * 
	 * 
	 * 
	 */
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text>{
    	public MyMapper() {
		}
    	@Override
    	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    			throws IOException, InterruptedException {
    		String[] s=value.toString().split(",");
    		String f1=s[0];
    		String f2=s[1];
    		//同时写入互为好友数据
    		context.write(new Text(f1),new Text(f2+"_ori"));
    		context.write(new Text(f2),new Text(f1+"_temp"));
    	}
    }
	/**
	 * A,B_ori
	 * B,A_tmp
	 * B,C_ori
	 * C,B_tmp
	 * C,F_ori
	 * F,C_temp
	 * 
	 * 分组后数据是
	 * A,B_ori
	 * ---------
	 * B,A_tmp
	 * B,C_ori
	 * ---------
	 * C,B_tmp
	 * C,F_ori
	 * ------
	 * F,C_temp
	 * 
	 * 
	 * reduce数据写入原始的数据
	 * A,B_ori
	 * B,C_ori
	 * C,F_ori
	 * 同时写入所有key相同fof数据
	 * A,C_fof
	 * B,F_fof
	 * 
	 * 
	 * @author jiaozi
	 *
	 */
    public static class MyReduce extends Reducer<Text, Text, Text, Text>{
    	public MyReduce() {
    		
		}
    	@Override
    	protected void reduce(Text key, Iterable<Text> value,
    			Reducer<Text, Text, Text, Text>.Context c) throws IOException, InterruptedException {
    		Iterator<Text> iterator = value.iterator();
    		List<String> allList=new ArrayList<String>();
    	    while(iterator.hasNext()){
    	    	Text v = iterator.next();
    	    	allList.add(v.toString());
    	    	if(v.toString().endsWith("_ori")){
    	    		c.write(key, new Text(v.toString()));
    	    	}
    	    }
    	    for(int i=0;i<allList.size();i++){
    	    	for(int j=i+1;j<allList.size();j++){
    	    		String f1=allList.get(i);
    	    		String f2=allList.get(j);
    	    		String f1Str=f1.toString().split("_")[0];
	    			String f2Str=f2.toString().split("_")[0];
	    	    	c.write(new Text(f1Str), new Text(f2Str.toString()+"_fof"));
        	    }
    	    }
    	    
    	}
    	
    }
    
    /**
     * 这个mapper就是要最终确定 fof是否已经是好友了 就需要 
     * A,B_ori
	 * B,C_ori
	 * C,F_ori
	 * A,C_fof
	 * B,F_fof
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
    		String[] sp=f2.split("_");
    		f2=sp[0];
    		String f2s=sp[1];
    		//同时写入互为好友数据
    		context.write(new Text(f1),new Text(f2+"_"+f2s));
    		context.write(new Text(f2),new Text(f1+"_"+f2s));
    		
    	}
    }
    public static class MyReduce1 extends Reducer<Text, Text, Text, Text>{
    	public MyReduce1() {
    		
		}
    	@Override
    	protected void reduce(Text key, Iterable<Text> value,
    			Reducer<Text, Text, Text, Text>.Context c) throws IOException, InterruptedException {
    		Iterator<Text> iterator = value.iterator();
    		Set<String> fofList=new HashSet<String>();
    		Set<String> oriList=new HashSet<String>();
    	    while(iterator.hasNext()){
    	    	Text v = iterator.next();
    	    	String vStr=v.toString();
    	    	String v1=vStr.split("_")[0];
    	    	if(vStr.endsWith("_ori")){
    	    		oriList.add(v1);
    	    	}else{
    	    		fofList.add(v1);
    	    	}
    	    }
    	    for(String fof1:fofList){
    	    	boolean ifFriend=false;
    	    	 for(String ori1:oriList){
    	    		if(fof1.equals(ori1)){
    	    			ifFriend=true;
    	    			break;
    	    		}
        	    }
    	    	if(!ifFriend){
    	    		c.write(key, new Text(fof1));
    	    	}
    	    }
    	    
    	}
    	
    }
    
    
    
}
