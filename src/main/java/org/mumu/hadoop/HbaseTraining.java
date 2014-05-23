package org.mumu.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
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
    private static HBaseAdmin admin;


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
                if(args.length != 6)
                //				tablePool = new HTablePool(hdfsUtil);
                    logger.info("Usage:tableName inputPath threadNumber startIndex endIndex");
                String inputPath = args[1];
                String tableName = args[2];
                int threadNumber = Integer.parseInt(args[3]);
                int startIndex = Integer.parseInt(args[4]);
                int endIndex = Integer.parseInt(args[5]);
                int fileNumber = endIndex - startIndex;
                HdfsUtil hdfsUtil = hbaseTraining.getHdfsUtil();
                Path path = new Path(inputPath);
                List<Path> fileList = hdfsUtil.getFileList(path);
                for(Path file:fileList){
                    logger.info("file in {}: {} ",inputPath,file.getName());
                }
                try {
                    admin = new HBaseAdmin(hdfsUtil.getConf());
                    if(!admin.tableExists(tableName)){
                        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
                        descriptor.addFamily(new HColumnDescriptor("info"));
                        admin.createTable(descriptor);
                    }
                    admin.close();
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                } 

                Thread[]  threadPool  =  new  Thread[threadNumber];
                int  filesToBeRead  =  fileList.size()  -  startIndex;  //  剩余需要读取的文件数量  
                while  (fileNumber  >  0  &&  filesToBeRead  >  0)  {  
                    if(filesToBeRead  <  threadNumber)  {  //  剩余文件数量小于线程数  
                        for(int  i  =  0;  i  <  filesToBeRead;  i++){  //  为每个文件创建一个导入线程  
                            threadPool[i]  =  new  HBaseImportThread(tableName, i,  inputPath + "/"+fileList.get(startIndex  +  i).getName(),    
                                    hbaseTraining.getHcon());  
                        }  
                        for(int  i  =  0;  i  <  filesToBeRead;  i++){  
                            try {
                                threadPool[i].join();
                            } catch (InterruptedException e){
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }  //  等待子线程结束后后续代码方可继续执行  
                        }
                        startIndex +=filesToBeRead;
                        fileNumber -= filesToBeRead;
                        filesToBeRead -= filesToBeRead;
                    }else{  //  剩余文件数量大于等于线程数  
                        for  (int  i  =  0;  i  <  threadNumber;  i++){  //  为每个文件创建一个导入线程  
                            threadPool[i]  =  new  HBaseImportThread(tableName, i,  inputPath + "/"+fileList.get(startIndex  +  i).getName(),    
                                    hbaseTraining.getHcon());  
                        }  
                        for(int  i  =  0;  i  <  threadNumber;  i++){  
                            try {
                                threadPool[i].join();
                            }catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }  //  等待子线程结束后后续代码方可继续执行  
                        }
                        startIndex +=threadNumber;
                        fileNumber -= threadNumber;
                        filesToBeRead -= threadNumber;
                    }
                }
                logger.info("multiThread: all jobs' done !");

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
                    logger.warn("upload takes 2 arguments :tableName , filePath");
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
                            String tableName1 = args[1];
                            ArrayList<Put> puts = new ArrayList<Put>();
                            try {
                                admin = new HBaseAdmin(hbaseTraining.getHdfsUtil().getConf());
                                if(!admin.tableExists(tableName1)){
                                    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName1));
                                    descriptor.addFamily(new HColumnDescriptor("info"));
                                    admin.createTable(descriptor);
                                }
                                admin.close();
                            } catch (IOException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                            }
                            while((line = (fileBufferedReader.readLine())) != null){
                                String[] split = line.split("\t");
                                for(String v : split)vals.add(v);
                                Put put = hbaseTraining.getHbaseUtil().addRecord(tableName1, key, "info", cols, vals);
                                puts.add(put);
                                if(count >0 && count % 8196 == 0){
                                    if(firstTime){
                                        logger.info("starting to load file: {} to table: {}",args[2],args[1]);
                                        firstTime = false;
                                    }
                                    hbaseTraining.getHbaseUtil().getTable().put(puts);
                                    logger.info("{} records inserted to {}",count,args[1]);
                                    puts.clear();
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
