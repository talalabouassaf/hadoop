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


public class query5 {
	static Configuration conf = HBaseConfiguration.create();

	public static void main(String[] args) throws IOException {
		System.out.println(getMoyFromProgYear("L1", "2007"));

	}

	@SuppressWarnings({ "resource", "deprecation" })
	public static String getMoyFromProgYear(String programme, String year) throws IOException {

		if (programme.length() == 2 && year.length() == 4) {
			HTable query5 = new HTable(conf, setting.getId()+":query5");
			String note = "";
			String reponse = "{";


			Scan scan = new Scan();
			RowFilter filter = new RowFilter(CompareOp.EQUAL,
					new RegexStringComparator("S[0-9]{2}[A-B][0-9]{3}" + "/" + year + "/" + programme));
			scan.setFilter(filter);
			ResultScanner resultScanner = query5.getScanner(scan);
			Result result = resultScanner.next();

			if (result == null) {
				return "NOT FOUND";
			}
			while (result != null) {
				List<Cell> cellsG = result.listCells();
				for (Cell cellG : cellsG) {

					if (Bytes.toString(CellUtil.cloneQualifier(cellG)).equals("M")) {
						note = Bytes.toString(CellUtil.cloneValue(cellG));
					}

					if (Bytes.toString(CellUtil.cloneQualifier(cellG)).equals("N")) {
						reponse += "\"" + Bytes.toString(CellUtil.cloneRow(cellG)).substring(0, 7) + "\":";
						reponse += "{\"Name\":" + Bytes.toString(CellUtil.cloneValue(cellG)) + "\",\"Grade\":\"" + note
								+ "\"}";
					}

				}
				result = resultScanner.next();
				if (result != null) {
					reponse += ",";
				}
			}
			reponse += "}";
			return reponse;
		} else {
			return "FORMAT PROGRAM INVALID (L1-M2) OR FORMAT YEAR INVALID (2000-2018) ";
		}

	}

}