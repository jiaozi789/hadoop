package hadoop.mr.cas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 1.输入 输入文件内容： 
 * 1949-10-01 14:21:02 34C 
 * 1949-10-02 14:21:12 36C 
 * 1950-02-02 11:21:12 32C
 * 1950-05-02 11:31:12 37C 
 * 1951-12-02 11:31:12 23C
 * 1950-12-02 11:31:12 47C 
 * 1950-12-02 11:31:12 27C 
 * 1951-06-02 11:31:12 48C 
 * 1951-07-02 11:31:12 45C
 *  数据是时间和温度的记录 其中时间yyyy-MM-dd HH:mm:ss后面跟的是制表符tab
 * 2. 输出 计算出： 1949-1951 年之间，每年温度最高的前K天（例如k=5）
 * 
 * @author jiaozi
 *
 */
public class TempoReduceTest {
	/**
	 * mapreduce过程是 将文件的逐行传入map方法  然后按照map输出的数据按照key进行排序【实现WritableComparable重写compareto实现】  
	 * 然后调用分区函数（setPartitionerClass指定）  然后对数据进行分组 （setGroupingComparatorClass指定）【默认是安装key进行分组 自定义了按照自定义的】
	 * 决定将来写入哪个reduce（默认1个） 如果指定排序 setSortComparatorClass就会替换实体类的compareto方法
	 *  
	 *  
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("c:/user/temper_output")))
			fs.delete(new Path("c:/user/temper_output"), true);
		Job job = Job.getInstance(conf, "Temp_Calc");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Tempo.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReduce.class);
		job.setGroupingComparatorClass(GroupCompartor.class);
		//job.setSortComparatorClass(GroupCompartor.class);
		job.setPartitionerClass(TempoPartitioner.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path("c:/user/temper_input"));
		FileOutputFormat.setOutputPath(job, new Path("c:/user/temper_output"));
		job.waitForCompletion(true);
	}
	public static class GroupCompartor extends WritableComparator{
		public GroupCompartor() {
			super(Tempo.class,true);
		}
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			try {
				Tempo t=(Tempo)a;
				Tempo t1=(Tempo)b;
				int curyear=sdf.parse(t.getDate()).getYear()+1900;
				int curyear1=sdf.parse(t1.getDate()).getYear()+1900;
				return curyear-curyear1;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return 0;
		}
	}
	
	
	public static class Tempo implements WritableComparable<Tempo> {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private String date;
		private int wd;
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
		public int getWd() {
			return wd;
		}
		public void setWd(int wd) {
			this.wd = wd;
		}
		/**
		 * 数据类型序列化写入
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(date);
			out.writeInt(wd);
		}
		/**
		 * 数据类型序列化读取
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			date=in.readUTF();
			wd=in.readInt();
		}
		/**
		 * map函数调用后 进行排序 自动调用这个方法
		 * 按照年份排序 按照温度排序
		 */
		@Override
		public int compareTo(Tempo o) {
			try {
				int curyear=sdf.parse(this.getDate()).getYear()+1900;
				int cyear=sdf.parse(o.getDate()).getYear()+1900;
				if(curyear!=cyear){
					return curyear-cyear;
				}
				return o.getWd()-this.getWd();
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return 0;
		}
		
		
	}

	public static class TempoPartitioner extends Partitioner<Tempo, IntWritable> {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		@Override
		public int getPartition(Tempo key, IntWritable value, int numPartitions) {
			int curyear=0;
			try {
				curyear = sdf.parse(key.getDate()).getYear()+1900;
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return curyear%numPartitions;
		}

	}
	public static class MyMapper extends Mapper<LongWritable,Text, Tempo, IntWritable>{
    	public MyMapper() {
		}
    	@Override
    	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Tempo, IntWritable>.Context context)
    			throws IOException, InterruptedException {
    		//1949-10-01 14:21:02 34C 
    		String line=value.toString();
    		int index=line.lastIndexOf("\t");
    		String date=line.substring(0, index);
    		String wd=line.substring(index+1);
    		Tempo tm=new Tempo();
    		int wdint=Integer.parseInt(wd.substring(0, wd.length()-1));
    		tm.setWd(Integer.parseInt(wd.substring(0, wd.length()-1)));
    		tm.setDate(date);
    		context.write(tm, new IntWritable(wdint));
    		
    	}
    }
    public static class MyReduce extends Reducer<Tempo, IntWritable, Text, IntWritable>{
    	public MyReduce() {
    		
		}
    	@Override
    	protected void reduce(Tempo arg0, Iterable<IntWritable> arg1,
    			Reducer<Tempo, IntWritable, Text, IntWritable>.Context c) throws IOException, InterruptedException {
    	    Iterator<IntWritable> iterator = arg1.iterator(); 
    	    System.out.println("----------------");
    		while(iterator.hasNext()){
    			IntWritable next = iterator.next();
    			System.out.println(arg0.getDate()+"-"+next);
    			c.write(new Text(arg0.getDate()),next);
    		}
    	}
    	
    }

}
