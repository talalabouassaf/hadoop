package reducer;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class reducer3 extends TableReducer<Text, Text, ImmutableBytesWritable> {
    

    @SuppressWarnings("deprecation")
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int total = 0;
        int nb = 0;

        for (Text vals : values) {
        	String[] listenote = vals.toString().split("/");
        		nb += Integer.parseInt(listenote[0]);
        		total += Integer.parseInt(listenote[1]);
        }
        
        float valu = (float) Math.round(((float) nb / total)*100)/100; 
        String key3 = key.toString();

        
        
        Put put = new Put(Bytes.toBytes(key3));
        put.add(Bytes.toBytes("#"),Bytes.toBytes("TR"),Bytes.toBytes(String.valueOf(valu)));
        
        
        context.write(null, put);
    }

     
}