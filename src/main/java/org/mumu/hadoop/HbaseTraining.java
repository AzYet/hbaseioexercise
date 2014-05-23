package org.mumu.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseTraining {

    public HbaseTraining(){
        super();
        hdfsUtil = new HdfsUtil();
        setHbaseUtil(new HbaseUtil());
        try {
            hcon = HConnectionManager.createConnection(hdfsUtil.getConf());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private HdfsUtil hdfsUtil;
    static Logger logger = LoggerFactory.getLogger(HbaseTraining.class);
    private HbaseUtil hbaseUtil;
    private HConnection hcon;

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args){
        HbaseTraining hbaseTraining = new HbaseTraining();
        logger.info("...................program HbaseTraining started.................");
        if(args.length == 0){
            logger.warn("arguments must be specified");
        }else{
            switch(args[0]){
            case "multiThread":
                //				tablePool = new HTablePool(hdfsUtil);
                break;
            case "test":
                hbaseTraining.getHbaseUtil().addRecord("test", 111, "info",
                        new ArrayList<String>(){
                    private static final long serialVersionUID = 1L;
                    {
                        add("q1");
                        add("q2");
                    }},
                    new ArrayList<String>(){
                        private static final long serialVersionUID = 1L;
                        {
                            add("q1");
                            add("q2");
                        }});
                break;
            case "fileToTable":
                if(args.length != 3){
                    logger.warn("upload takes 2 arguments :tableNmae , filePath");
                }else{
                    BufferedReader fileBufferedReader = hbaseTraining.hdfsUtil.getFileBufferedReader(args[2]);
                    if(fileBufferedReader != null){
                        logger.info("read file {} succeed",args[2]);
                        String line;
                        try {
                            String firstLine = fileBufferedReader.readLine();
                            List<String> cols = new ArrayList<String>();
                            List<String> vals = new ArrayList<String>();
                            if(firstLine != null){
                                String[] split = firstLine.split("\t");
                                for(String c : split)cols.add(c); 
                            }
                            int key = 0;
                            int count = 1;
                            boolean firstTime = true;
                            ArrayList<Put> puts = null;
                            while((line = (fileBufferedReader.readLine())) != null){
                                puts = new ArrayList<Put>();
                                String[] split = line.split("\t");
                                for(String v : split)vals.add(v);
                                Put put = hbaseTraining.getHbaseUtil().addRecord(args[1], key, "info", cols, vals);
                                puts.add(put);
                                if(count >0 && count % 8196 == 0){
                                    if(firstTime){
                                        logger.info("starting to load file: {} to table: {}",args[2],args[1]);
                                        firstTime = false;
                                    }
                                    hbaseTraining.getHbaseUtil().getTable().put(puts);
                                    logger.info("{} records inserted to {}",count,args[1]);
                                    puts = new ArrayList<Put>();
                                }
                                count++;
                                key++;
                            }
                            if(!puts.isEmpty()){
                                hbaseTraining.getHbaseUtil().getTable().put(puts);
                            }
                            logger.info("convert finished: total {} records inserted to {}",count-1,args[1]);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }else{
                        logger.info("read hdfs file fail");
                    }
                }
                break;
            case "uploadFile":
                if(args.length != 3){
                    logger.warn("upload takes 2 arguments");
                }else{
                    if(hbaseTraining.hdfsUtil.uploadFile(args[1], args[2])){
                        logger.info("upload file succeed");
                    }else{
                        logger.info("upload file fail");
                    }
                }
                break;
            case "print":
                if(args.length != 2){
                    logger.warn("upload takes 1 arguments");
                }else{
                    BufferedReader fileBufferedReader = hbaseTraining.hdfsUtil.getFileBufferedReader(args[1]);
                    int count = 0;
                    String line = null;
                    try {
                        while(count <10 && (line = fileBufferedReader.readLine()) != null){
                            System.out.println(line);
                            count++;
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                break;
            default:break;
            }
        }
    }
    
    public void multiThreadTest(String[] args){
        try {
            HTableInterface table = hcon.getTable("test");
            if(args.length<4){//程序使用方法提示
                System.out.println("Usage:inputPath threadNumber poolSize startIndex endIndex");
                System.exit(2);
                }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    public HbaseUtil getHbaseUtil() {
        return hbaseUtil;
    }

    public void setHbaseUtil(HbaseUtil hbaseUtil) {
        this.hbaseUtil = hbaseUtil;
    }

    public HdfsUtil getHdfsUtil() {
        return hdfsUtil;
    }

    public void setHdfsUtil(HdfsUtil hdfsUtil) {
        this.hdfsUtil = hdfsUtil;
    }

    public HConnection getHcon() {
        return hcon;
    }

    public void setHcon(HConnection hcon) {
        this.hcon = hcon;
    }

}
