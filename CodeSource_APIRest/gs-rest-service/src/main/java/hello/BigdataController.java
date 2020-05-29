package hello;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.*;


@RestController
public class BigdataController {


    public static Configuration conf = HBaseConfiguration.create();

    //QUERY 1
    @SuppressWarnings({ "resource", "unused", "deprecation" })
    public static String getquery1(String student, String programme) throws IOException {
        HTable table = new HTable(conf, "21301866:S");

        String[] prog = new String[2];
        ArrayList<String> info = new ArrayList<String>();
        String reponse;

        if (student.length() != 10 || programme.length() != 2) {
            return "NOT FOUND";
        }

        Get get = new Get(Bytes.toBytes(student));
        get.addFamily(Bytes.toBytes("#"));
        get.addFamily(Bytes.toBytes("C"));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        if (Bytes.toString(result.getValue(Bytes.toBytes("#"),Bytes.toBytes("L")))==null) {
            return "NOT FOUND";
        }
        for (Cell cell : cells) {
            info.add(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        HTable query1 = new HTable(conf, "21301866:query1");
        Get getnote = new Get(Bytes.toBytes(student+"/"+programme));
        Result resultnote = query1.get(getnote);

        if (Bytes.toString(resultnote.getValue(Bytes.toBytes("#"),Bytes.toBytes("SS")))  == null || Bytes.toString(resultnote.getValue(Bytes.toBytes("#"),Bytes.toBytes("FS"))) == null) {
            return "NOT FOUND";
        }

        String Fsem = "\"First\":[";
        String Ssem = "\"Second\":[";
        for (String note : Bytes.toString(resultnote.getValue(Bytes.toBytes("#"),Bytes.toBytes("FS"))).split(";")) {
            String [] notes = note.split("/");
            Fsem += "{\"Code\":\"" + notes[0] + "\",\"Name\":\"" + notes[1] + "\",\"Grade\":\"" + notes[2].substring(0, 2) + "." + notes[2].substring(2, notes[2].length())
                    + "\"},";
        }

        for (String note : Bytes.toString(resultnote.getValue(Bytes.toBytes("#"),Bytes.toBytes("SS"))).split(";")) {
            String [] notes = note.split("/");
            Ssem += "{\"Code\":\"" + notes[0] + "\",\"Name\":\"" + notes[1] + "\",\"Grade\":\"" + notes[2].substring(0, 2) + "." + notes[2].substring(2, notes[2].length())
                    + "\"},";
        }

        reponse = "{\"Name\": \"" + info.get(0) + " " + info.get(1) + "\", \"Email\": \"" + info.get(5)
                + "\", \"Programme\": \"" + programme + "\"," + Fsem.substring(0, Fsem.length()-1) + "]," + Ssem.substring(0, Ssem.length()-1) + "]}";
        table.close();

        return reponse;
    }

    //QUERY 2
    public static String getquery2(String semestre) throws IOException {
        HTable table = new HTable(conf, "21301866:query2");
        String reponse ="[";

        if(semestre.length()==2) {

            Scan scan = new Scan();
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("[0-9]{4}"+"/"+semestre));
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

    //QUERY 3
    public static String getquery3(String id) throws IOException {

        HTable table = new HTable(conf, "21301866:query3");
        String reponse ="[";

        if(id.length()==7) {

            Scan scan = new Scan();
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(id+"/"+".*"));
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

    //QUERY 4
    public static String getquery4(String id,String year) throws IOException {

        HTable table = new HTable(conf, "21301866:query4");
        String reponse;
        if(id.length()==7 && year.length()==4) {
            Get get = new Get(Bytes.toBytes(id+"/"+year));
            Result result = table.get(get);
            if(Bytes.toString(result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N")))==null) {
                reponse = "NOT FOUND";
            }else{
                byte[] name = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
                byte[] taux = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("NT"));

                reponse = "{\"Name\":\""+Bytes.toString(name)+"\",\"Rate\":\"" +Bytes.toString(taux)+"\"}";
            }

        }else {
            return "FORMAT INVALID (id=S00A000 year=2000)";
        }
        return reponse;
    }

    //QUERY 5
    public static String getquery5(String programme, String year) throws IOException {

        if (programme.length() == 2 && year.length() == 4) {
            HTable query5 = new HTable(conf, "21301866:query5");
            String note = "";
            String reponse = "{";


            Scan scan = new Scan();
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
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
                        reponse += "{\"Name\":\"" + Bytes.toString(CellUtil.cloneValue(cellG)) + "\",\"Grade\":\"" + (double) Math.round(Double.parseDouble(note) * 100) / 100
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

    //QUERY 6
    public static String getquery6(String intructor) throws IOException {


        HTable query6 = new HTable(conf, "21301866:query6");
        String tr = "";
        String reponse = "{";

        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
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
                    reponse += "\"" + valeurs[0] + "/"+valeurs[1]+"\":{\"Name\":\""+ Bytes.toString(CellUtil.cloneValue(cellG)) + "\",\"Rate\":\""+tr+"\"}";

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

    //QUERY 7
    public static String getquery7(String programme, String year) throws IOException {

        if (programme.length() == 2 && year.length() == 4) {
            HTable query7 = new HTable(conf, "21301866:query7");

            String reponse = "{";

            Scan scan = new Scan();
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
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

    //http://localhost:8080/students/2018000999/transcripts/L1
    @RequestMapping(value = "/students/{id}/transcripts/{program}", method = RequestMethod.GET,produces = "application/json")
    public Object query1(@PathVariable("id") String id,@PathVariable("program") String program) throws IOException {
        return getquery1(id,program);

    }

    //http://localhost:8080/rates/03
    @RequestMapping(value = "/rates/{semester}", method = RequestMethod.GET,produces = "application/json")
    public Object query2(@PathVariable("semester") String semester) throws IOException {
        return getquery2(semester);

    }

    //http://localhost:8080/courses/S07A010/rates
    @RequestMapping(value = "/courses/{id}/rates", method = RequestMethod.GET,produces = "application/json")
    public Object query3(@PathVariable("id") String id) throws IOException {
        return getquery3(id);

    }

    //http://localhost:8080/courses/S07A010/rates/2010
    @RequestMapping(value = "/courses/{id}/rates/{year}", method = RequestMethod.GET,produces = "application/json")
    public Object query4(@PathVariable("id") String id, @PathVariable("year") String year) throws IOException {
        return getquery4(id,year);

    }

    //http://localhost:8080/programs/L1/means/2010
    @RequestMapping(value = "/programs/{program}/means/{year}", method = RequestMethod.GET,produces = "application/json")
    public Object query5(@PathVariable("program") String program, @PathVariable("year") String year) throws Exception {
        return getquery5(program,year);

    }

    //http://localhost:8080/instructors/Obkcy%20Bdhuoeb/rates
    @RequestMapping(value = "/instructors/{name}/rates", method = RequestMethod.GET,produces = "application/json")
    public Object query6(@PathVariable("name") String name) throws IOException {
        return getquery6(name);

    }

    //http://localhost:8080/ranks/L1/years/2010
    @RequestMapping(value = "/ranks/{program}/years/{year}", method = RequestMethod.GET,produces = "application/json")
    public Object query7(@PathVariable("program") String program,@PathVariable("year") String year) throws IOException {
        return getquery7(program,year);

    }
}
