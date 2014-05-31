package org.mumu.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
	private static Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

	public Put addRecord(String tableName,int key,String cf, 
			List<String> quanlifiers, List<String> vals){
		try {
			table = new HTable(hbaseConf, tableName);
			Put put = new Put(Bytes.toBytes(key+""));
			
			for(int i=0;i < quanlifiers.size();i++){
				put.add(Bytes.toBytes(cf), Bytes.toBytes(quanlifiers.get(i)),
						Bytes.toBytes(vals.get(i)));
			}
			return put;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public void selectByRowKey(String tablename,String rowKey) throws IOException{  
        table=new HTable(hbaseConf,tablename);  
        Get g = new Get(Bytes.toBytes(rowKey));  
        Result r=table.get(g);  
        List<Cell> cells = r.listCells();
        for(KeyValue kv : r.raw()){
            System.out.println("column: "+new String(kv.getFamily()) +",qualifier: "+new String(kv.getQualifier()));  
            System.out.println("value: "+new String(kv.getValue()));  
        }

        /*for(Cell cell:cells){
        	byte[] familyArray = cell.getFamilyArray();
        	int familyOffset = cell.getFamilyOffset();
        	byte familyLength = cell.getFamilyLength();
        	byte[] familyBytes = new byte[familyLength];
        	System.arraycopy(familyArray, familyOffset, familyBytes, 0, familyLength);
        	byte[] valueArray = cell.getValueArray();
        	int valueOffset = cell.getValueOffset();
        	int valueLength = cell.getValueLength();
        	byte[] valueBytes = new byte[valueLength];
        	System.arraycopy(valueArray, valueOffset, valueBytes, 0, valueLength);
        	byte[] qualifierArray = cell.getQualifierArray();
        	int qualifierOffset = cell.getQualifierOffset();
        	int qualifierLength = cell.getQualifierLength();
        	byte[] qualifierBytes = new byte[qualifierLength];
        	System.arraycopy(qualifierArray, qualifierOffset, qualifierBytes, 0, qualifierLength);

//        	cell.getQualifierArray()
//        	ByteArray.
            logger.info("column: "+new String(familyBytes));  
            logger.info("qualifier: "+new String(qualifierBytes));  
            logger.info("value: "+new String(valueArray));  
        }*/
    }  
	
	public void selectByFilter(String tablename,List<String> arr) throws IOException{  
        HTable table=new HTable(hbaseConf,tablename);  
        FilterList filterList = new FilterList();  
        Scan s1 = new Scan();  
        for(String v:arr){ // 各个条件之间是“与”的关系  
            String [] s=v.split(",");  
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]),  
                                                             Bytes.toBytes(s[1]),  
                                                             CompareOp.EQUAL,Bytes.toBytes(s[2])  
                                                             )  
            );  
            // 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回  
//          s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));  
        }  
        s1.setFilter(filterList);  
        ResultScanner resultScannerFilterList = table.getScanner(s1);  
        for(Result rr=resultScannerFilterList.next();rr!=null;rr=resultScannerFilterList.next()){  
            for(KeyValue kv:rr.list()){  
                System.out.println("column: "+new String(kv.getFamily()) +",qualifier: "+new String(kv.getQualifier()));  
                System.out.println("value: "+new String(kv.getValue()));  
            }  
            
            System.out.println("total "+rr.size()+" records.");  
            /*List<Cell> cells = rr.listCells();
            for(Cell cell:cells){
            	byte[] familyArray = cell.getFamilyArray();
            	int familyOffset = cell.getFamilyOffset();
            	byte familyLength = cell.getFamilyLength();
            	byte[] familyBytes = new byte[familyLength];
            	System.arraycopy(familyArray, familyOffset, familyBytes, 0, familyLength);
            	byte[] valueArray = cell.getValueArray();
            	int valueOffset = cell.getValueOffset();
            	int valueLength = cell.getValueLength();
            	byte[] valueBytes = new byte[valueLength];
            	System.arraycopy(valueArray, valueOffset, valueBytes, 0, valueLength);
            	byte[] qualifierArray = cell.getQualifierArray();
            	int qualifierOffset = cell.getQualifierOffset();
            	int qualifierLength = cell.getQualifierLength();
            	byte[] qualifierBytes = new byte[qualifierLength];
            	System.arraycopy(qualifierArray, qualifierOffset, qualifierBytes, 0, qualifierLength);

//            	cell.getQualifierArray()
//            	ByteArray.
                logger.info("column: "+new String(familyBytes));  
                logger.info("qualifier: "+new String(qualifierBytes));  
                logger.info("value: "+new String(valueArray));  
            }*/
        }  
    }  
	
	public void selectByRowKeyColumn(String tablename,String rowKey,String column, String qualifier) throws IOException{  
        HTable table=new HTable(hbaseConf,tablename);  
        Get g = new Get(Bytes.toBytes(rowKey));  
        g.addColumn(Bytes.toBytes(column), Bytes.toBytes(qualifier));  
        Result r=table.get(g);  
        for(KeyValue kv:r.raw()){  
            System.out.println("column: "+new String(kv.getFamily()) +",qualifier: "+new String(kv.getQualifier()));  
            System.out.println("value: "+new String(kv.getValue()));  
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
