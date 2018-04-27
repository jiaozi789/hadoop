package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MyLength extends UDF {
	public Text evaluate(final Text s) {
      if(s==null) { 
			return new Text("0"); 
		}
		return new Text(s.toString().length()+"");
	}

}
