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
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import setup.setting;


public class query6 {
	
	static Configuration conf = HBaseConfiguration.create();
	
	@SuppressWarnings({ "deprecation", "resource" })
	public static String getTRFromInstructor(String intructor) throws IOException {

			
				HTable query6 = new HTable(conf, setting.getId()+":query6");
				String tr = "";
				String reponse = "{";

				Scan scan = new Scan();
				RowFilter filter = new RowFilter(CompareOp.EQUAL,
						new RegexStringComparator(".*"+"/" + intructor));
				scan.setFilter(filter);
				ResultScanner resultScanner = query6.getScanner(scan);
				Result result = resultScanner.next();

				if (result == null) {
					return "NOT FOUND";
				}
				while (result != null) {
					List<Cell> cellsG = result.listCells();
					for (Cell cellG : cellsG) {
						String[] valeurs = Bytes.toString(CellUtil.cloneRow(cellG)).split("/");
						if (Bytes.toString(CellUtil.cloneQualifier(cellG)).equals("TR")) {
							tr = Bytes.toString(CellUtil.cloneValue(cellG));
						}

						if (Bytes.toString(CellUtil.cloneQualifier(cellG)).equals("N")) {
							reponse += "\"" + valeurs[0] + "/"+valeurs[1]+"\":\"{\"Name\":\""+ Bytes.toString(CellUtil.cloneValue(cellG)) + "\",\"Rate\":\""+tr+"\"}";
					
						}
						
					}
					result = resultScanner.next();
					if (result != null) {
						reponse += ",";
					}
				}
				reponse += "}";
				query6.close();
				return reponse;
			}
		
		
	public static void main(String[] args) throws IOException{
		System.out.println(getTRFromInstructor("Sqanj Gpylhi"));
        
    }

}