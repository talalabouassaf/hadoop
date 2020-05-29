package job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import reducer.*;
import setup.setting;
import mapper.*;

public class job7{
	
    
	 @SuppressWarnings({ "deprecation", "resource" })
	    public static void main(String[] args) throws Exception {
	    Configuration config = HBaseConfiguration.create();
	    Job job = new Job(config,"A");
	    job.setJarByClass(map7.class);  
	    String sourceTable = setting.getId()+":G";
	    String targetTable = setting.getId()+":query7";
	    
	    HBaseAdmin admin = new HBaseAdmin(config);
	    if(admin.tableExists(targetTable)) {
	        admin.disableTable(targetTable);
	        admin.deleteTable(targetTable);
	    }
	    
	    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(targetTable));
	    tableDescriptor.addFamily(new HColumnDescriptor("#"));
	    admin.createTable(tableDescriptor);
	    
	    Scan scan = new Scan();

	    TableMapReduceUtil.initTableMapperJob(sourceTable, scan, map7.class, Text.class, Text.class, job);
	    TableMapReduceUtil.initTableReducerJob(targetTable,reducer7.class,job);
	    job.setNumReduceTasks(1);
	    
	    boolean b = job.waitForCompletion(true);
	    if (!b) {
	        throw new IOException("error with job!");
	    }
    
    }
}