package org.mumu.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseUtil {
	public HbaseUtil() {
		super();
		hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", "u1");
	}

	private Configuration hbaseConf;
	private HTable table;
//	private static Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

	public Put addRecord(String tableName,int key,String cf, 
			List<String> quanlifiers, List<String> vals){
		try {
			table = new HTable(hbaseConf, tableName);
			Put put = new Put(Bytes.toBytes(key+""));
			
			for(int i=0;i < quanlifiers.size();i++){
				put.add(Bytes.toBytes(cf), Bytes.toBytes(quanlifiers.get(i)),
						Bytes.toBytes(vals.get(i)));
				i++;
			}
			return put;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public HTable getTable() {
		return table;
	}

	public void setTable(HTable table) {
		this.table = table;
	}
	

}
