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
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import setup.setting;


public class query2 {
	
	public static Configuration conf = HBaseConfiguration.create();
	
	@SuppressWarnings({ "deprecation", "resource" })
	public static String getTRFromSemestre(String semestre) throws IOException {
        HTable table = new HTable(conf, setting.getId()+":query2");
        String reponse ="[";
        
        if(semestre.length()==2) {
        	  
        	Scan scan = new Scan();
	        RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("[0-9]{4}"+"/"+semestre));
	        scan.setFilter(filter);
	        ResultScanner resultScanner = table.getScanner(scan);
	        Result resultG = resultScanner.next();
	        if(resultG==null) {
	        	return "NOT FOUND";
	        }
	        while (resultG != null) {	
	        	List<Cell> cellsG = resultG.listCells();
	        	 for (Cell cellG : cellsG) {
	        		reponse +=  "{\"Year\":\""+Bytes.toString(CellUtil.cloneRow(cellG)).substring(0, 4)+"\",\"Rate\":\"" + Bytes.toString(CellUtil.cloneValue(cellG))+"\"}";
	        	 }
	        	 resultG = resultScanner.next();
	        	 if(resultG != null) {
	        		 reponse += ",";
	        	 }
	        }
        }else {
        	return "FORMAT INVALID (00-10)";
        }
        reponse += "]";
        return reponse;
      
     
	}


	public static void main(String[] args) throws IOException{
        
		System.out.println(getTRFromSemestre("07"));
        
    }

}