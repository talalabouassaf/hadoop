package query;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import setup.setting;


public class query4 {
public static Configuration conf = HBaseConfiguration.create();
	
	@SuppressWarnings({ "deprecation", "resource" })
	public static String getTauxRFromIdYear(String id,String year) throws IOException {
		
		HTable table = new HTable(conf, setting.getId()+":query4");
        String reponse;
        if(id.length()==7 && year.length()==4) {
        	Get get = new Get(Bytes.toBytes(id+"/"+year));
            Result result = table.get(get);
            if(result==null) {
            	reponse = "NOT FOUND";
            }
            byte[] name = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
            byte[] taux = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("NT"));
           
            reponse = "{\"Name\":\""+Bytes.toString(name)+"\",\"Rate\":\"" +Bytes.toString(taux)+"\"}";
        }else {
        	return "FORMAT INVALID (id=S00A000 year=2000)";
        }
        return reponse;
	}
		
	public static void main(String[] args) throws IOException{
        System.out.println(getTauxRFromIdYear("S07A010","2010"));
    }
}