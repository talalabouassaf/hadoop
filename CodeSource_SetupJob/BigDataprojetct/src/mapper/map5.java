package mapper;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class map5 extends TableMapper<Text, Text> {

	private Text text = new Text();

	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		byte[] values = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));

		String val = Bytes.toString(values);
		String[] key2 = Bytes.toString(key.get()).split("/");
		String programme= key2[1].substring(0, 2);
		String prog = "";
		
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
		String newkey = key2[2] + "/" + key2[0] + "/" +prog;
		text.set(val);

		context.write((new Text(newkey)), text);

	}
}
