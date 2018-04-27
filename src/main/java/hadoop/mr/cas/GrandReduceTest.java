package hadoop.mr.cas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

import hadoop.mr.cas.TempoReduceTest.TempoPartitioner;

/**
 * 参考文档Hadoop案例之单表关联输出祖孙关系.docx
 * 实例中给出child-parent（孩子——父母）表，要求输出grandchild-grandparent（孙子——爷奶）表。
    样例输入如下所示。
	child        parent
	Tom        Lucy
	Tom        Jack
	Jone        Lucy
	Jone        Jack
	Lucy        Mary
	Lucy        Ben
	Jack        Alice
	Jack        Jesse
	Terry        Alice
	Terry        Jesse
	Philip        Terry
	Philip        Alma
	Mark        Terry
	Mark        Alma
	
	解决思路 就是 每个关系 正反插入两条数据到map
	Tom-Luck  child-parent
	Luck-Tom  parent-child  reduce是 key相同的就是交叉的
	Luck-Mary child-parent
	luck-Bean child-parent
	Tom和Mary必定祖孙关系
	
 * @author jiaozi
 *
 */
public class GrandReduceTest {
	public static void main(String[] args) throws Exception {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("c:/user/grand_output")))
			fs.delete(new Path("c:/user/grand_output"), true);
		Job job = Job.getInstance(conf, "Grand_Calc");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReduce.class);
		//job.setSortComparatorClass(GroupCompartor.class);
		//job.setPartitionerClass(TempoPartitioner.class);
		//job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path("c:/user/grand_input"));
		FileOutputFormat.setOutputPath(job, new Path("c:/user/grand_output"));
		job.waitForCompletion(true);
	}
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text>{
    	public MyMapper() {
		}
    	@Override
    	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    			throws IOException, InterruptedException {
    		String[] s=value.toString().split("\t");
    		String child=s[0];
    		String parent=s[1];
    		//0表示当前key的parent是value
    		context.write(new Text(child),new Text(0+"_"+parent));
    		//1表示当前key的child是value
    		context.write(new Text(parent),new Text(1+"_"+child));
    	}
    }
    public static class MyReduce extends Reducer<Text, Text, Text, Text>{
    	public MyReduce() {
    		
		}
    	@Override
    	protected void reduce(Text key, Iterable<Text> value,
    			Reducer<Text, Text, Text, Text>.Context c) throws IOException, InterruptedException {
    		Iterator<Text> iterator = value.iterator();
    		List<Text> grandsonList=new ArrayList<Text>();
    		List<Text> grandList=new ArrayList<Text>();
    	    while(iterator.hasNext()){
    	    	Text v = iterator.next();
    	    	String ch=v.toString().substring(2);
    	    	if(v.toString().startsWith("0_")){
    	    		grandList.add(new Text(ch));
    	    	}
    	    	if(v.toString().startsWith("1_")){
    	    		grandsonList.add(new Text(ch));
    	    	}
    	    }
    	    for(Text grand:grandList){
    	    	 for(Text grandSon:grandsonList){
    	    	    	c.write(grand, grandSon);
    	    	 }
    	    }
    	    
    	    
    	    
    	}
    	
    }
}
