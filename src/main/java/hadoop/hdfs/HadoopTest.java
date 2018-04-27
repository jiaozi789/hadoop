package hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HadoopTest {
	public static FileSystem getSystem() throws IOException {
		// 设置当前机器的hadoop目录
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		// 设置操作使用的用户 如果不设置为root 和 hadoop服务的相同 出现异常 本机账号是window账号
		System.setProperty("HADOOP_USER_NAME", "root");
		String dst = "hdfs://192.168.58.147:9000";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		return fs;
	}
	/**
	 * 创建目录
	 * @param dir 目录名
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void mkdir(String dir) throws IllegalArgumentException, IOException{
		FileSystem fs= getSystem();
		fs.mkdirs(new Path(dir));
		fs.close();
	}
	/**
	 * 查看某个目录下所有的文件和目录
	 * @param dir 父目录
	 * @throws IOException
	 */
	public static void listFile(String dir) throws IOException{
		FileSystem fs= getSystem();
		FileStatus[]  rtls=fs.listStatus(new Path(dir));
		for(FileStatus fst:rtls){
			String path=fst.getPath().toString();
			System.out.println(path);
		}
		
	}
	/**
	 * 上传文件
	 * @param localFile 本地需要上传文件的绝对路径
	 * @param destDir 需要上传的目标目录
	 * @throws IOException 
	 */
	public static void upload(String localFile,String destDir) throws IOException{
		FileSystem fs= getSystem();
		if(!destDir.endsWith("/")){
			destDir=destDir+"/";
		}
		File localF=new File(localFile);
		OutputStream os=fs.create(new Path(destDir+localF.getName()));
		InputStream is=new FileInputStream(localF);
		IOUtils.copyBytes(is, os, 2048,true);
	}
	/**
	 * 下载hdfs文件到本地
	 * @param hdfsSrc hdfs需要下载的源文件 比如 /ttt/a.html
	 * @param localDest 本地文件存储的文件全路径 比如 c:/b.html
	 * @throws IOException 
	 */
	public static void download(String hdfsSrc,String localDest) throws IOException{
		FileSystem fs= getSystem();
		InputStream is=fs.open(new Path(hdfsSrc));
		OutputStream os=new FileOutputStream(localDest);
		IOUtils.copyBytes(is, os, 2048, true);
	}
	/**
	 * 删除目录
	 * @param hdfssrc 目录路径
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void rmdir(String hdfssrc) throws IllegalArgumentException, IOException{
		FileSystem fs= getSystem();
		fs.delete(new Path(hdfssrc), true);
	}
	
	
	public static void main(String[] args) throws Exception {
		listFile("/");
		//upload("c:/a.html","/ttt");
		//download("/ttt/a.html","c:/b.htm");
		rmdir("/ttt");
	}
}
