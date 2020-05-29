package query;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import setup.setting;

import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;


public class query3 {
	
	public static Configuration conf = HBaseConfiguration.create();
	
	@SuppressWarnings({ "deprecation", "resource" })
	public static String getTauxRFromId(String id) throws IOException {
		
		HTable table = new HTable(conf, setting.getId()+":query3");
        String reponse ="[";
        
        if(id.length()==7) {
        	  
        	Scan scan = new Scan();
	        RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(id+"/"+".*"));
	        scan.setFilter(filter);
	        ResultScanner resultScanner = table.getScanner(scan);
	        Result resultG = resultScanner.next();
	        if(resultG==null) {
	        	return "NOT FOUND";
	        }
	        while (resultG != null) {	
	        	List<Cell> cellsG = resultG.listCells();
	        	 for (Cell cellG : cellsG) {
	        		 String[] keyf = Bytes.toString(CellUtil.cloneRow(cellG)).split("/");
	        		reponse +=  "{\"Name\":\""+keyf[1]+"\",\"Rate\":\"" + Bytes.toString(CellUtil.cloneValue(cellG))+"\"}";
	        	 }
	        	 resultG = resultScanner.next();
	        	 if(resultG != null) {
	        		 reponse += ",";
	        	 }
	        }
        }else {
        	return "FORMAT INVALID (S00A000)";
        }
        reponse += "]";
        return reponse;
	}
		
	public static void main(String[] args) throws IOException{
        System.out.println(getTauxRFromId("S07A010"));
    }

}