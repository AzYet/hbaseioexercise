package org.mumu.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseImportThread extends Thread{
        private HdfsUtil hdfsUtil;
        private  int  threadIndex;  //  线程索引  
        private  HConnection  hconn;  //  数据库连接池  
        private  HTableInterface  table;  //  表对象  
        private BufferedReader reader;
        private String tableName;
        static Logger logger = LoggerFactory.getLogger(HBaseImportThread.class);
        public  HBaseImportThread(String tableName, int  threadIndex,  String  fileName,  HConnection  hconn)  {  
            logger.info("Thread  index  is  "  +  threadIndex  +  ".  File  name  is  "  +  fileName);  
            hdfsUtil = new HdfsUtil();
            this.reader  =  hdfsUtil.getFileBufferedReader(fileName);  
            this.threadIndex  =  threadIndex;  //  保存线程索引  
            this.hconn  =  hconn;  //  保存表连接池  
            this.tableName = tableName;
            start();  //  启动线程  
        }

        public  void  run()  {  
            try {
                table  =  hconn.getTable(tableName);
                //  开启表  
                //            table.setAutoFlush(false);  //  deprecated method   
                table.setAutoFlushTo(false);  //  禁用自动提交  
                String line;
                while  ((line = reader.readLine()) != null)  {  //  逐行读取数据文件  
                    String[]  columnList = 
                            new String[]{"time","userID","serverIP","hostName","spName","uploadTraffic","downloadTraffic"};
                    String[] parts;
                    if  ((parts = line.split("\t")).length  ==  columnList.length)  {  //  判断数据格式合法性  
                        ArrayList<byte[]> colNameList = new ArrayList<byte[]>();
                        for(int i= 0 ; i < colNameList.size();i++){
                            colNameList.add(Bytes.toBytes(columnList[i]));    
                        }
                        //                    no unique
                        //                    byte[]  rowKey  =  new  byte[userID.length  +  time.length];  //  以userID+time构造rowkey  
                        //                    use whole line's hash value as key 
                        byte[] rowKey = (parts[0]+"-"+parts[1]+"-"+Cypher.getMD5(parts)).getBytes();    
                        //                    Bytes.putBytes(rowKey,  0,  userID,  0,  userID.length);  
                        //                    Bytes.putBytes(rowKey,  userID.length,  time,  0,  time.length);  
                        Put  put  =  new  Put(rowKey);  //  put数据  
                        int i = 0;
                        for(String colName:columnList){
                            put.add("info".getBytes(),  colName.getBytes(),  parts[i].getBytes());  
                            i++;
                        }
                        table.put(put);
                    }else {
                       logger.info("thread index {}: bad line : {}",threadIndex,line);
                    }
                }
                table.flushCommits();
                table.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }