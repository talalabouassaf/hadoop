package reducer;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import setup.setting;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class reducer1 extends TableReducer<Text, Text, ImmutableBytesWritable>  {

	Configuration conf = HBaseConfiguration.create();
	
 	@SuppressWarnings({ "deprecation", "static-access" })
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 		HTable courses = new HTable(conf, setting.getId()+":query4");
 		
 		ArrayList<String> Fsemestre = new ArrayList<String>();
 		ArrayList<String> Ssemestre = new ArrayList<String>();
 		
    	for (Text vals : values) {
    		String[] Valtable = vals.toString().split("/");
    		 Get get = new Get(Bytes.toBytes(Valtable[1]+"/"+Valtable[3]));
    	       Result resultget = courses.get(get);
    	        byte [] newnom = resultget.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"));
    	        String newn = Bytes.toString(newnom);
    		if(Valtable[0].equals("01") || Valtable[0].equals("03") || Valtable[0].equals("05") || Valtable[0].equals("07") || Valtable[0].equals("09")) {
    			Fsemestre.add(Valtable[1]+"/"+ newn +"/"+ Valtable[2]);
    		}
    		if(Valtable[0].equals("02") || Valtable[0].equals("04") || Valtable[0].equals("06") || Valtable[0].equals("08") || Valtable[0].equals("10")) {
    			Ssemestre.add(Valtable[1]+"/"+ newn +"/"+ Valtable[2]);
    		}
		}
    	
    	Put put = new Put (Bytes.toBytes(key.toString()));
    	put.add(Bytes.toBytes("#"),Bytes.toBytes("FS"),Bytes.toBytes(String.valueOf(String.join(";",Fsemestre))));
    	put.add(Bytes.toBytes("#"),Bytes.toBytes("SS"),Bytes.toBytes(String.valueOf(String.join(";",Ssemestre))));
    	context.write(null, put);
   	}
 	
 	
}