package mapper;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class map1 extends TableMapper<Text, Text> {

	
	private Text text = new Text();

	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		byte[] values = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
		

		String val = Bytes.toString(values);
		String[] key2 = Bytes.toString(key.get()).split("/");
		String prog = "";
		String programme= key2[1].substring(0, 2);
		
		if(programme.equals("01") || programme.equals("02")) {
            prog = "L1";
            }
		if(programme.equals("03") || programme.equals("04")) {
            prog = "L2";
            }
		if(programme.equals("05") || programme.equals("06")) {
            prog = "L3";
            }
		if(programme.equals("07") || programme.equals("08")) {
            prog = "M1";
            }
		if(programme.equals("09") || programme.equals("10")) {
            prog = "M2";
            }
		
		String newkey = key2[1].substring(2)+"/"+prog;
		
		text.set(programme+"/"+key2[2]+"/"+val+"/"+key2[0]);

		context.write((new Text(newkey)), text);

	}
}