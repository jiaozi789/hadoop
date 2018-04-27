package hadoop.mr.format;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;

public class ConnectionUtils {
	static Connection connection=null;
	public static Connection getConnection(Configuration configuration) throws Exception{
		String driverClassName=configuration.get(MyFileInputForm.DB_DRIVERCLASS);
		String url=configuration.get(MyFileInputForm.DB_URL);
		String userName=configuration.get(MyFileInputForm.DB_USERNAME);
		String password=configuration.get(MyFileInputForm.DB_PASSWORD);
		Class.forName(driverClassName);
		Connection connection = DriverManager.getConnection(url,userName,password);
		return connection;
	}
}
