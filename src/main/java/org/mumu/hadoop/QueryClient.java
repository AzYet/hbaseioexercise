package org.mumu.hadoop;


public class QueryClient implements Runnable{

    private String fileName;
	private long startTime;
	private long endTime;
	private String userID;
	private String zoneID;
	private String traffic;
	/**
     * @param args
     */
    public static void main(String[] args) {
        QueryClient client = new QueryClient(); 
        client.fileName= args[0]; // 结果文件路径 
        for (int i= 1; i< args.length; i++) { 
            String colConditionIndex= args[i]; // 查询条件索引 
            String colConditionStr= args[++i]; // 查询条件字符串 
            if (colConditionIndex.equals("0")) { // 起始时间 
                client.startTime= Long.parseLong(colConditionStr); 
            } else if (colConditionIndex.equals("1")) { // 结束时间 
                client.endTime= Long.parseLong(colConditionStr); 
            } else if (colConditionIndex.equals("2")) { // 用户ID 
                client.userID= colConditionStr; 
            } else if (colConditionIndex.equals("3")) { // 小区ID 
                int index = colConditionStr.indexOf("_"); 
                client.zoneID= getZoneID(colConditionStr.substring(0, index).getBytes(), 
                        colConditionStr.substring(index + 1).getBytes()); 
            } else if (colConditionIndex.equals("4")) { // 下行流量 
                client.traffic= getTraffic(colConditionStr.getBytes()); 
            } 
        } 
        client.run(); // 执行查询 
    }
    private static String getTraffic(byte[] bytes) {
		// TODO Auto-generated method stub
		return null;
	}
	private static String getZoneID(byte[] bytes, byte[] bytes2) {
		// TODO Auto-generated method stub
		return null;
	}
	public void run(){ 
        if (startTime == 0 || endTime == 0) { // 起始时间和结束时间是必选条件 
            return; 
        } 
        /*if (zoneID== null && traffic == null) { // 仅使用用户ID查询 
            QueryByUserIDThread query=new QueryByUserIDThread(startTime,endTime,userID,fileName); 
            query.start(); 
        } else if (zoneID!= null && traffic != null) { // 使用zoneID和traffic查询 
            QueryByZoneIDAndTrafficThread query = new QueryByZoneIDAndTrafficThread(startTime, 
                    endTime, traffic, zoneID, fileName); 
            query.start(); 
        } else if (zoneID!= null) { // 仅使用zoneID查询 
            QueryByZoneIDThread query=new QueryByZoneIDThread(startTime,endTime,zoneID,fileName); 
            query.start(); 
        } else if (traffic != null) { // 仅使用traffic查询 
            QueryByTrafficThread query = new QueryBytednThread(startTime, endTime, traffic, fileName); 
            query.start(); 
        } */
    }
	public String getZoneID() {
		return zoneID;
	}
	public String getTraffic() {
		return traffic;
	}
}