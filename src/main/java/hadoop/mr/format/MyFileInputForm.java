package hadoop.mr.format;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
/**
 * 首先必须从 Configuration中获取 数据库信息
 * 统计某个数据库的表 以及每张表的数据量
   
 * @author jiaozi
 *
 * @param <K>
 * @param <V>
 */
public class MyFileInputForm<K, V> extends InputFormat<K, V> {
	public static final String DB_DRIVERCLASS="cn.et.db.driverClass";
	public static final String DB_URL="cn.et.db.url";
	public static final String DB_NAME="cn.et.db.name";
	public static final String DB_USERNAME="cn.et.db.userName";
	public static final String DB_PASSWORD="cn.et.db.password";
	public static final String DB_TYPE="cn.et.db.type";
	public static final int DB_SPLIT_NUM=5;
	
	/**
	 * 分片的实现  注意一定要实现Writable接口否则报错
	 * @author jiaozi
	 */
	static class MyFileInputSplit extends InputSplit  implements Writable  {
		private  int start;
		private  int end;
		//注意一定要有个默认构造方法 用于网络传输 自动实例化
		public MyFileInputSplit(){
		}
		//代码实例化调用这个构造方法
		public MyFileInputSplit(int start, int end) {
			this.start = start;
			this.end = end;
		}

		//1个数据库 就一个分片
		@Override
		public long getLength() throws IOException, InterruptedException {
			return end-start;
		}
		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}
		public int getStart() {
			return start;
		}
		public int getEnd() {
			return end;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(start);
			out.writeInt(end);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.start=in.readInt();
			this.end=in.readInt();
		}
	}
	
	/**
	 * 分片就按照数据库来进行分片
	 */    
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		List<InputSplit> lis=new ArrayList<InputSplit>();
		try {
			Connection connection = ConnectionUtils.getConnection(configuration);
			String dbName=configuration.get(DB_NAME);
			String sql="SELECT COUNT(*) as MC FROM information_schema.TABLES where TABLE_SCHEMA = '"+dbName+"';";
			String dbType=configuration.get(DB_TYPE);
			if(dbType.equals("oracle")){
				sql="select count(*) as MC from tab";
			}
			ResultSet executeQuery = connection.createStatement().executeQuery(sql);
			int count=0;
			while(executeQuery.next()){
				count=executeQuery.getInt("MC");
			}
			executeQuery.close();
			connection.close();
			int result=count/DB_SPLIT_NUM;
			int loopCount=count%DB_SPLIT_NUM==0?result:result+1;
			for(int i=0;i<loopCount;i++){
				int start=i*DB_SPLIT_NUM;
				int end=(i+1)*DB_SPLIT_NUM;
				if(i==(loopCount-1) &&count%DB_SPLIT_NUM!=0){
					 end=count;
				}
				MyFileInputSplit mi=new MyFileInputSplit(start,end);
				lis.add(mi);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lis;
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		MyFileRecordReader mrr=new  MyFileRecordReader();
		return mrr;
	}
	/**
	 * 从当前的块读取数据 
	 * @author jiaozi
	 *
	 * @param <K>
	 * @param <V>
	 */
	public static class MyFileRecordReader<K,V> extends RecordReader<IntWritable, Text>{
		private  MyFileInputSplit fileSplit;
		private Configuration conf;
		private Connection conn=null;
		private int index;
		private String tableName;
		private List<String> blockList=null;
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			fileSplit=(MyFileInputSplit)split;
			conf=context.getConfiguration();
			index=fileSplit.getStart();
			try {
				conn=ConnectionUtils.getConnection(conf);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//判断同一个分块是否存在下一个值
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int start=fileSplit.getStart();//0
			int end=fileSplit.getEnd();//5
				//一次性将这一split的数据都查询出来 查过一次就不用查第二次
			if(blockList==null){
				blockList=new ArrayList<String>();
				String dbName=conf.get(DB_NAME);
				String sql="SELECT table_name as MC FROM information_schema.TABLES where TABLE_SCHEMA = '"+dbName+"' limit "+start+","+end;
				String dbType=conf.get(DB_TYPE);
				if(dbType.equals("oracle")){
					sql="select * from (select tname as MC,rownum as rn from tab) t where t.rn>="+(start+1)+" and t.rn <="+end;
				}
				try {
					ResultSet executeQuery = conn.createStatement().executeQuery(sql);
					while(executeQuery.next()){
						tableName=executeQuery.getString("MC");
						blockList.add(tableName);
					}
					executeQuery.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(index>=start && index<end){
				index++;
				return true;
			}
			index++;
			return false;
		}
		@Override
		public IntWritable getCurrentKey() throws IOException, InterruptedException {
			int start=fileSplit.getStart();//0	
			return new IntWritable(index-start-1);
		}
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new Text(blockList.get(getCurrentKey().get()));
		}
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return getCurrentKey().get()/fileSplit.getLength();
		}
		@Override
		public void close() throws IOException {
			try {
				conn.close();
				conn=null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		
		
	}
}

