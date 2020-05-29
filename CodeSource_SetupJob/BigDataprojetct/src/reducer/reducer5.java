package reducer;
import java.io.IOException;
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

public class reducer5 extends TableReducer<Text, Text, ImmutableBytesWritable>  {

	public static Configuration conf = HBaseConfiguration.create();
	
 	@SuppressWarnings({ "deprecation" })
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 		HTable courses = new HTable(conf, setting.getId()+":C");
 		float total=0;
 		int nb = 0;
    	for (Text vals : values) {
    		total += Float.parseFloat(vals.toString())/100;
			nb++;
		}
    	
    	String[] key2 = key.toString().split("/");
    	int year = 9999-Integer.parseInt(key2[1]);
    	String code = key2[0];
		
    	float valu = total/nb;
    	Put put = new Put (Bytes.toBytes(key.toString()));
    	put.add(Bytes.toBytes("#"),Bytes.toBytes("M"),Bytes.toBytes(String.valueOf(valu)));
    	put.add(Bytes.toBytes("#"),Bytes.toBytes("N"),Bytes.toBytes(String.valueOf(foundname(code,year,courses,true))));
    	context.write(null, put);
    	courses.close();
   	}
 	
 	public String foundname(String key,int year,HTable courses,Boolean flag) throws IOException {
 		Get get = new Get(Bytes.toBytes(key+"/"+year));
		Result resultget = courses.get(get);
		 byte [] newnom = resultget.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"));
		 String newn = Bytes.toString(newnom);
		 if(newn == null || newn.equals("") || newn.equals("null")) {
			 if(year==7998) {
				 flag=false;
				 return foundname(key,year-1,courses,flag);
			 }else {
				 if(!flag) {
					 return foundname(key,year-1,courses,flag);
				 }else {
					 return foundname(key,year+1,courses,flag);
				 }
			 }
		 }
 		return newn;
 	}

}
   
