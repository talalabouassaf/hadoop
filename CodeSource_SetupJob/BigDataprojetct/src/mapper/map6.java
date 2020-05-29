package mapper;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class map6 extends TableMapper<Text, Text> {

	private Text text = new Text();

	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		byte[] values = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("NT"));
		byte[] names = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));

		String val = Bytes.toString(values);
		String newkey = Bytes.toString(key.get());
		text.set(val);

		context.write((new Text(newkey+"/"+Bytes.toString(names))), text);
		
	}
}
