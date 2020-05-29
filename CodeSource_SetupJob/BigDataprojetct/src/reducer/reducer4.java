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

public class reducer4 extends TableReducer<Text, Text, ImmutableBytesWritable> {
	Configuration conf = HBaseConfiguration.create();

	@SuppressWarnings("deprecation")
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HTable courses = new HTable(conf, setting.getId()+":C");
		
		int cumule = 0;
		int total = 0;
		int nb = 0;

		for (Text vals : values) {
			if (Float.parseFloat(vals.toString()) >= 1000) {
				nb++;
				cumule =+ nb;
			}
			total++;
		}
		
		float valu = (float) Math.round(((float) nb / total)*100)/100;		
		String[] key2 = key.toString().split("/");
		String matiere = key2[0];
		int year = Integer.valueOf(key2[1]);
		String key3 = matiere + "/"+ (9999-year);

		
		
		Put put = new Put(Bytes.toBytes(key3));
		put.add(Bytes.toBytes("#"),Bytes.toBytes("N"),Bytes.toBytes(String.valueOf(foundname(matiere,year,courses,true))));
		put.add(Bytes.toBytes("#"),Bytes.toBytes("NT"),Bytes.toBytes(String.valueOf(valu)));
		put.add(Bytes.toBytes("#"),Bytes.toBytes("C"),Bytes.toBytes(String.valueOf(cumule)));
		put.add(Bytes.toBytes("#"),Bytes.toBytes("T"),Bytes.toBytes(String.valueOf(total)));
		
		courses.close();
		context.write(null, put);
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
