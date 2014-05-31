package org.mumu.hadoop;

public class QueryByUserIDThread {

    /**
     * @param args
     */
    public QueryByUserIDThread(long startTime, long endTime, String userID, String fileName) { 
        /*this.startTime= startTime; // 起始时间查询条件 
        this.endTime= endTime; // 结束时间查询条件 
        this.userID= userID; // 用户ID查询条件 
        this.output= new FileOutputStream(fileName); // 结果文件 
*/    } 
    public void run() { 
        /*int threadNum = (int) (endTime- startTime) / 3600 + 1; // 每小时的一张表对应一个线程
        signal = new CountDown(threadNum); // 初始化计数器 
        for (int i= 0; i< threadNum; i++) { // 逐个启动查询线程 
            QueryThread query = new QueryThread(startTime+ i* 3600, userID, tablePool, signal); 
            new Thread(query).start(); 
        } 
        while (true) { 
            writeResult(); // 保存结果 
            if (signal.getCount()== 0) break; // 全部查询线程执行完毕后退出 
        } 
        exit(); */
    }
}
