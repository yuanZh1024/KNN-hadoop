package com.yuanzh.dataJoin.RatingsAndUsers;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private HashMap<String,String> user_info=new HashMap<String,String>();
	private String splitter="";
	private String rating_secondPart="";
	/*
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		splitter=context.getConfiguration().get("SPLITTER");
		Path[] distributePaths=DistributedCache.getLocalCacheFiles(context.getConfiguration());//@Deprecated
		String line="";
		BufferedReader br=null;
		for (Path path : distributePaths) {
			if(path.toString().endsWith("users.dat")){
				br=new BufferedReader(new FileReader(path.toString()));
				while((line=br.readLine())!=null){
					String userID=line.substring(0, line.indexOf("::"));
					String secondPart=line.substring(line.indexOf("::")+2,line.length());
					user_info.put(userID, secondPart);
				}
			}
		}
	}*/

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		splitter = context.getConfiguration().get("SPLITTER");

		Configuration conf  = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);

		URI[] paths = context.getCacheFiles();
		String line = "";
		BufferedReader br = null;
		for(URI path : paths ){
			if(path.toString().endsWith("users.dat")){
				//br = new BufferedReader(new FileReader("/Users/yuanzh/Documents/workspace/github/KNN-hadoop/data/users.dat"));
				//LocalCacheFiles弃用后，不能使用FileReader初始化hdfs上文件，现在的cachefiles在hdfs上，所以
				//用javaAPI操作hdfs文件即可。或者如上，直接使用本地文件
				Path path2 = new Path(path.toString());
				FSDataInputStream is = fs.open(path2);
				br = new BufferedReader(new InputStreamReader(is));
				while((line=br.readLine())!=null){
					String userID = line.substring(0,line.indexOf("::"));
					String secondPart = line.substring(line.indexOf("::")+2,line.length());
					user_info.put(userID,secondPart);
				}
			}
		}
	}
	//map接收的是ratings.dat文件
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] val = value.toString().split(splitter);
		rating_secondPart = user_info.get(val[0]);
		if(rating_secondPart != null){
			String result = val[0]+splitter+rating_secondPart+splitter+val[1];
			context.write(new Text(result), NullWritable.get());
		}
	}


}
