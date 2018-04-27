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
		// ���õ�ǰ������hadoopĿ¼
		System.setProperty("hadoop.home.dir", "D:\\learn\\hadoop\\hadoop-2.7.4");
		// ���ò���ʹ�õ��û� ���������Ϊroot �� hadoop�������ͬ �����쳣 �����˺���window�˺�
		System.setProperty("HADOOP_USER_NAME", "root");
		String dst = "hdfs://192.168.58.147:9000";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		return fs;
	}
	/**
	 * ����Ŀ¼
	 * @param dir Ŀ¼��
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public static void mkdir(String dir) throws IllegalArgumentException, IOException{
		FileSystem fs= getSystem();
		fs.mkdirs(new Path(dir));
		fs.close();
	}
	/**
	 * �鿴ĳ��Ŀ¼�����е��ļ���Ŀ¼
	 * @param dir ��Ŀ¼
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
	 * �ϴ��ļ�
	 * @param localFile ������Ҫ�ϴ��ļ��ľ���·��
	 * @param destDir ��Ҫ�ϴ���Ŀ��Ŀ¼
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
	 * ����hdfs�ļ�������
	 * @param hdfsSrc hdfs��Ҫ���ص�Դ�ļ� ���� /ttt/a.html
	 * @param localDest �����ļ��洢���ļ�ȫ·�� ���� c:/b.html
	 * @throws IOException 
	 */
	public static void download(String hdfsSrc,String localDest) throws IOException{
		FileSystem fs= getSystem();
		InputStream is=fs.open(new Path(hdfsSrc));
		OutputStream os=new FileOutputStream(localDest);
		IOUtils.copyBytes(is, os, 2048, true);
	}
	/**
	 * ɾ��Ŀ¼
	 * @param hdfssrc Ŀ¼·��
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
