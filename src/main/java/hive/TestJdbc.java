package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.ql.processors.SetProcessor;

public class TestJdbc {
	public static void main(String[] args) throws Exception {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		//;fetchSize=10000;hive.server2.thrift.resultset.default.fetch.size=10
		Connection con = DriverManager.getConnection("jdbc:hive2://192.168.58.147:10000/default", "root", "");
		Statement stmt = con.createStatement();
		String tableName = "testtable";
		stmt.execute("drop table if exists " + tableName);
		stmt.execute("create table " + tableName + " (key int, value string)");
		System.out.println("Create table success!");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
	}
}
