package mapper;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class map2 extends TableMapper<Text, Text> {

	private Text text = new Text();

	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		byte[] values = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));

		String val = Bytes.toString(values);
		String[] key2 = Bytes.toString(key.get()).split("/");
		String newkey = key2[0] + "/" + key2[1].substring(0, 2);
		text.set(val);

		context.write((new Text(newkey)), text);

	}
}
