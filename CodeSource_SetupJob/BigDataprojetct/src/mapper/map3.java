package mapper;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class map3 extends TableMapper<Text, Text> {

    private Text text = new Text();
    
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
    	
       byte[] name = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
       byte[] cumule = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("C"));
       byte[] total = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("T"));

       String name1 = Bytes.toString(name);
       String cumuleval = Bytes.toString(cumule);
       String totalval = Bytes.toString(total);
       
       String[] key2 = Bytes.toString(key.get()).split("/");
       
       String newkey = key2[0]+"/"+name1;
       
       text.set(cumuleval+"/"+totalval);
       
       

       context.write((new Text(newkey)), text);

   }
}