package mapper;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class map4 extends TableMapper<Text, Text> {

	 private Text text = new Text();
	 
	 public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		byte[] values = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));

		String val = Bytes.toString(values);
		String[] key2 = Bytes.toString(key.get()).split("/");
		int year=9999-Integer.valueOf(key2[0]);
		String newkey = key2[2]+"/"+year;
		text.set(val);
		
		

		context.write((new Text(newkey)), text);

	}
	 
	 
}