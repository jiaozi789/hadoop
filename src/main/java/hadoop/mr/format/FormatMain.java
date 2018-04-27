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
 * ��Ҫͳ��ĳ�����ݿ�ı� �Լ�ÿ�ű��������
 * ��������Ŀ¼������һ���ļ� 
 *    a  110
 *    b  10
 * 
 * InputFormat ���𽫱���з�Ƭ ÿ5�ű�һ����Ƭ
 * OutputFormat �������reduce�ۺϵĽ�� ��������Ŀ¼�µ�һ���ļ���   
 *    
 *    
 * @author jiaozi
 *
 */
public class FormatMain {


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// ���õ�ǰ������hadoopĿ¼
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
		//�]��reduce map�ĽY��ֱ��ݔ��
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
