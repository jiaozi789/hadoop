package hadoop.mr.format;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 需要统计某个数据库的表 以及每张表的数据量
 * 最后在这个目录下生成一个文件 
 *    a  110
 *    b  10
 * 
 * InputFormat 负责将表进行分片 每5张表一个分片
 * OutputFormat 负责将最红reduce聚合的结果 输出到这个目录下的一个文件中   
 *    
 *    
 * @author jiaozi
 *
 */
public class FormatMain {


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		Configuration conf = new Configuration();
		conf.set(MyFileInputForm.DB_DRIVERCLASS, "com.mysql.jdbc.Driver");
		conf.set(MyFileInputForm.DB_URL, "jdbc:mysql://localhost:3306/mysql");
		conf.set(MyFileInputForm.DB_USERNAME, "root");
		conf.set(MyFileInputForm.DB_PASSWORD, "");
		conf.set(MyFileInputForm.DB_TYPE, "mysql");
		conf.set(MyFileInputForm.DB_NAME, "mysql");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("c:/user/my_output")))
			fs.delete(new Path("c:/user/my_output"),true);
		Job job =Job.getInstance(conf, "WordCount");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//沒有reduce map的結果直接輸出
		//job.setReducerClass(MyReduce.class);
		job.setInputFormatClass(MyFileInputForm.class);
		FileOutputFormat.setOutputPath(job, new Path("c:/user/my_output"));
		job.waitForCompletion(true);
	}
	public static class MyMapper extends Mapper<IntWritable,Text, Text, IntWritable>{
    	public MyMapper() {
		}
    	@Override
    	protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, Text, IntWritable>.Context context)
    			throws IOException, InterruptedException {
    		try {
				Connection connection = ConnectionUtils.getConnection(context.getConfiguration());
				String sql="select count(*) as MC from "+value.toString();
				ResultSet executeQuery = connection.createStatement().executeQuery(sql);
				int count=0;
				while(executeQuery.next()){
					count=executeQuery.getInt("MC");
				}
				context.write(value,new IntWritable(count));
				executeQuery.close();
				connection.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    	}
    }
   
}
