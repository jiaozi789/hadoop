package hadoop.mr.cas;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 参考文档Hadoop案例之倒排索引.docx
 * 样例输入如下所示。
    1）file1：
 
		MapReduce is simple
 
    2）file2：
 
		MapReduce is powerful is simple
 
    3）file3：
 
		Hello MapReduce bye MapReduce
 
    样例输出如下所示。
 
	MapReduce      file1.txt:1;file2.txt:1;file3.txt:2;
	is        　　　　file1.txt:1;file2.txt:2;
	simple        　  file1.txt:1;file2.txt:1;
	powerful   　　 file2.txt:1;
	Hello       　　 file3.txt:1;
	bye       　　   file3.txt:1;
	
	思路 ： 所有单词 可以使用
	    单词  文件名 写入到reduce 然后再到reduce去统计 不同文件名次数 但是这样 如果某个单词在同一个文件出现了多次 就会传入两个相同键值对到reduce中 
	    可以使用 combine本地合并 
	    单词:文件名  次数来传递  本地合并 相同 次数+1
	
	
	
	
	
	 * @author jiaozi
 *
 */
public class ReverseIndexTest {

	public static void main(String[] args) throws Exception {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("c:/user/ri_output")))
			fs.delete(new Path("c:/user/ri_output"), true);
		Job job = Job.getInstance(conf, "Ri_Calc");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReduce.class);
		FileInputFormat.addInputPath(job, new Path("c:/user/ri_input"));
		FileOutputFormat.setOutputPath(job, new Path("c:/user/ri_output"));
		job.waitForCompletion(true);
	}
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text>{
    	public MyMapper() {
		}
    	@Override
    	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    			throws IOException, InterruptedException {
    		FileSplit fs=(FileSplit)context.getInputSplit();
    		//获取文件名
    		String name = fs.getPath().getName();
    		String[] keyArray=value.toString().split(" ");
    		for (String string : keyArray) {
    			context.write(new Text(string), new Text(name));
			}
    	}
    }
    public static class MyReduce extends Reducer<Text, Text, Text, Text>{
    	public MyReduce() {
		}
    	@Override
    	protected void reduce(Text key, Iterable<Text> value,
    			Reducer<Text, Text, Text, Text>.Context c) throws IOException, InterruptedException {
    		 Iterator<Text> iterator = value.iterator();
    		 Map<String,Integer> map=new HashMap();
    		 while(iterator.hasNext()){
     	    	Text v = iterator.next();
     	    	if(map.containsKey(v.toString())){
     	    		 Integer integer = map.get(v.toString());
     	    		 map.put(v.toString(), integer+1);
     	    	}else{
     	    		map.put(v.toString(), 1);
     	    	}
    		 }
    		 StringBuffer sb=new StringBuffer();
    		 for(Map.Entry<String,Integer> me:map.entrySet()){
    			 sb.append(me.getKey()+":"+me.getValue()+";");
    		 }
    		 c.write(key, new Text(sb.toString()));
    	}
    	
    }

}
