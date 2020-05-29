package query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import setup.setting;

import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

@SuppressWarnings("unused")
public class query1 {
	public static Configuration conf = HBaseConfiguration.create();

	@SuppressWarnings({ "deprecation", "resource" })
	public static String getEtudiant(String student, String programme) throws IOException {
		HTable table = new HTable(conf, setting.getId()+":S");

		String[] prog = new String[2];
		ArrayList<String> info = new ArrayList<String>();
		ArrayList<String> note = new ArrayList<String>();
		String reponse;

		if (programme.equals("L1")) {
			prog[0] = "01";
			prog[1] = "02";
		}
		if (programme.equals("L2")) {
			prog[0] = "03";
			prog[1] = "04";
		}
		if (programme.equals("L3")) {
			prog[0] = "05";
			prog[1] = "06";
		}
		if (programme.equals("M1")) {
			prog[0] = "07";
			prog[1] = "08";
		}
		if (programme.equals("M2")) {
			prog[0] = "09";
			prog[1] = "10";
		}

		if (student.length() != 10 || programme.length() != 2 || prog[0] == null) {
			return "NOT FOUND";
		}

		Get get = new Get(Bytes.toBytes(student));
		get.addFamily(Bytes.toBytes("#"));
		get.addFamily(Bytes.toBytes("C"));
		Result result = table.get(get);
		List<Cell> cells = result.listCells();
		if (result == null) {
			return "NOT FOUND";
		}
		for (Cell cell : cells) {
			info.add(Bytes.toString(CellUtil.cloneValue(cell)));
		}

		String Fsem = NoteSemestre(prog[0], student,true);
		String Ssem = NoteSemestre(prog[1], student,false);
		if(Fsem == null || Ssem == null) {
			return "NOT FOUND";
		}
		reponse = "{\"Name\": \"" + info.get(0) + " " + info.get(1) + "\", \"Email\": \"" + info.get(5)
				+ "\", \"Programme\": \"" + programme + "\"," + Fsem + "," + Ssem + "}";
		table.close();

		return reponse;
	}

	@SuppressWarnings({ "deprecation", "resource" })
	public static String NoteSemestre(String semestre, String student , Boolean flag) throws IOException {
		String Notes = "";
		String matiere;
		HTable nameC = new HTable(conf, setting.getId()+":query4");
		HTable grade = new HTable(conf, setting.getId()+":G");
		if (flag) {
			Notes = "\"First\":[";
		}else {
			Notes = "\"Second\":[";
		}
		Scan scan = new Scan();
		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("[0-9]{4}" + "/" + semestre + student+"/"+".*"));
		scan.setFilter(filter);
		ResultScanner resultScanner = grade.getScanner(scan);
		Result resultG = resultScanner.next();
		if(resultG == null) {
			return null;
		}
		while (resultG != null) {
			List<Cell> cellsG = resultG.listCells();
			for (Cell cellG : cellsG) {
				String[] keyn = Bytes.toString(CellUtil.cloneRow(cellG)).split("/");
				Get get = new Get(Bytes.toBytes(keyn[2]+"/"+keyn[0]));
	            Result result = nameC.get(get);
	            byte[] name = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
				String moyenne = Bytes.toString(CellUtil.cloneValue(cellG)).substring(0, 2) + "."
						+ Bytes.toString(CellUtil.cloneValue(cellG)).substring(2,
								Bytes.toString(CellUtil.cloneValue(cellG)).length());
				Notes += "{\"Code\":\"" + keyn[2] + "\",\"Name\":\"" + Bytes.toString(name) + "\",\"Grade\":\"" + moyenne
						+ "\"}";
			}
			resultG = resultScanner.next();
			if (resultG != null) {
				Notes += ",";
			}
		}
		grade.close();
		nameC.close();
		Notes += "]";
		return Notes;
	}

	public static void main(String[] args) throws IOException {

		System.out.println(getEtudiant("2001000001", "L1"));

	}

}