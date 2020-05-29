package reducer;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class reducer7 extends TableReducer<Text, Text, ImmutableBytesWritable> {
	

	@SuppressWarnings("deprecation")
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		

		int total = 0;
		int nb = 0;

		for (Text vals : values) {
			total+=Float.parseFloat(vals.toString());
			nb++;
		}
		
		
		float valu1 = (float) total/nb;
		int valu=(int) (valu1);
		
		String note= String.format("%04d", valu).substring(0, 2)+"."+String.format("%04d", valu).substring(2);
		
		String key3 = String.format("%04d", (2000-valu))+"/"+key.toString();

	
		Put put = new Put(Bytes.toBytes(key3));
		put.add(Bytes.toBytes("#"),Bytes.toBytes("M"),Bytes.toBytes(note));
		
		context.write(null, put);
	}

	
	 
}
