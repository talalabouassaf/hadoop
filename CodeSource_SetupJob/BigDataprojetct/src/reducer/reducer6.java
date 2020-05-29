package reducer;

import java.io.IOException;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.Text;

import setup.setting;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class reducer6 extends TableReducer<Text, Text, ImmutableBytesWritable> {

	public static Configuration conf = HBaseConfiguration.create();

	@SuppressWarnings({ "deprecation" })
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		HTable courses = new HTable(conf, setting.getId()+":C");
		
		String valfinal = "";
		String[] key2 = key.toString().split("/");
		String matiere = key2[0];
		int year = Integer.valueOf(key2[1]);
		String TEST = foundkey(matiere,9999-year,courses,true);
	
		Get get = new Get(Bytes.toBytes(TEST));
		Result result = courses.get(get);
			String[] uouo = getColumnsInColumnFamily(result, "I");
			if (uouo != null) {
				for (String string : uouo) {
					byte[] newnom = result.getValue(Bytes.toBytes("I"), Bytes.toBytes(string));
					String newn = Bytes.toString(newnom);
					
					for (Text vals : values) {
						valfinal = vals.toString();
					}
					
					String key3 = matiere + "/" + year + "/" + newn;
					Put put = new Put(Bytes.toBytes(key3));
					put.add(Bytes.toBytes("#"), Bytes.toBytes("TR"), Bytes.toBytes(String.valueOf(valfinal)));
					put.add(Bytes.toBytes("#"), Bytes.toBytes("N"), Bytes.toBytes(String.valueOf(key2[2])));
					
					context.write(null, put);
				}
			}
		courses.close();
	}

	public String[] getColumnsInColumnFamily(Result r, String ColumnFamily) {

		NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Bytes.toBytes(ColumnFamily));
		if (familyMap == null) {
			return null;
		}
		String[] Quantifers = new String[familyMap.size()];

		int counter = 0;
		for (byte[] bQunitifer : familyMap.keySet()) {
			Quantifers[counter++] = Bytes.toString(bQunitifer);

		}

		return Quantifers;
	}
	
	public String foundkey(String key,int year,HTable courses,Boolean flag) throws IOException {
        Get get6 = new Get(Bytes.toBytes(key+"/"+year));
       Result resultget = courses.get(get6);
        byte [] newnom = resultget.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"));
        String newn = Bytes.toString(newnom);
        if(newn == null || newn.equals("") || newn.equals("null")) {
            if(year==7998) {
                flag=false;
                return foundkey(key,year-1,courses,flag);
            }else {
                if(!flag) {
                    return foundkey(key,year-1,courses,flag);
                }else {
                    return foundkey(key,year+1,courses,flag);
                }
            }
        }
        return key+"/"+year;
    }

}
