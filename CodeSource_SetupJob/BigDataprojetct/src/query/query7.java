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

public class query7 {
	static Configuration conf = HBaseConfiguration.create();

	public static void main(String[] args) throws IOException {
		System.out.println(getRankStudentByProgYear("L1", "2010"));

	}

	@SuppressWarnings({ "deprecation", "resource" })
	public static String getRankStudentByProgYear(String programme, String year) throws IOException {

		if (programme.length() == 2 && year.length() == 4) {
			HTable query7 = new HTable(conf, setting.getId()+":query7");

			String reponse = "{";

			Scan scan = new Scan();
			RowFilter filter = new RowFilter(CompareOp.EQUAL,
					new RegexStringComparator("[0-9]{4}" + "/" + "[0-9]{10}" + "/" + year + "/" + programme));
			scan.setFilter(filter);
			ResultScanner resultScanner = query7.getScanner(scan);
			Result result = resultScanner.next();

			if (result == null) {
				return "NOT FOUND";
			}
			while (result != null) {
				List<Cell> cellsG = result.listCells();
				for (Cell cellG : cellsG) {

					reponse += "\"" + Bytes.toString(CellUtil.cloneRow(cellG)).substring(5, 15) + "\":\""
							+ Bytes.toString(CellUtil.cloneValue(cellG)) + "\"";

				}
				result = resultScanner.next();
				if (result != null) {
					reponse += ",";
				}
			}
			reponse += "}";
			query7.close();
			return reponse;
		} else {
			return "FORMAT PROGRAM INVALID (L1-M2) OR FORMAT YEAR INVALID (2000-2018) ";
		}

	}

}