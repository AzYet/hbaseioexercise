package org.mumu.hadoop;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * Hello world!
 *
 */
public class HdfsUtil 
{
	private static final String HDFS_URL = "hdfs://u1:9000";

	public HdfsUtil(){
		super();
		this.conf = new Configuration();  
		try {
			this.fileSystem = FileSystem.get(URI.create(HDFS_URL), conf);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	private Configuration conf;
	private FileSystem fileSystem;

	public boolean mkdir(String pathName){	
		try {
			Path path = new Path(pathName);  
			fileSystem.create(path);
			fileSystem.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	public boolean rmdir(String pathName){	
		try {
			Path path = new Path(pathName);  
			fileSystem.delete(path, true);
			fileSystem.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	public boolean uploadFile(String srcFile, String dstPath){	
		try {
			Path src = new Path(srcFile);  
			Path dst = new Path(dstPath);  
			fileSystem.copyFromLocalFile(src, dst);  
			fileSystem.close(); 
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	FileSystem getFileSystem() throws IOException{
		return fileSystem;
	}

	public BufferedReader getFileBufferedReader(String fileName){
		try {
			FSDataInputStream inStream = fileSystem.open(new Path(URI.create(fileName)));
			return new BufferedReader(new InputStreamReader(inStream));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Path> getFileList(Path path){
        ArrayList<Path> pathList = new ArrayList<Path>();
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            for(FileStatus fs : status){
                if(fs.isFile()){
                    pathList.add(fs.getPath());
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	    return pathList;
	}
	
	public Configuration getConf() {
		return conf;
	}
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
