package com.yuanzh.dataJoin.UsersAndMovies;

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

public class UsersMoviesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private HashMap<String,String> movie_info=new HashMap<String,String>();
	private String splitter="";
	private String movie_secondPart="";
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		//Path[] DistributePaths=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		URI[] paths = context.getCacheFiles();
		Configuration conf = context.getConfiguration();
		splitter=conf.get("SPLITTER");
		String line="";
		BufferedReader br=null;
		for (URI path : paths) {
			if(path.toString().endsWith("movies.dat")){
				FileSystem fs = FileSystem.get(conf);
				Path path2 = new Path(path.toString());
				FSDataInputStream is = fs.open(path2);
				//br=new BufferedReader(new FileReader(path.toString()));
				br = new BufferedReader(new InputStreamReader(is));
				while((line=br.readLine())!=null){
					String movieID=line.split(splitter)[0];
					String genres=line.split(splitter)[2];
					movie_info.put(movieID, genres);
				}
			}
		}
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] val=value.toString().split(splitter);
		movie_secondPart=movie_info.get(val[5]);
		if(movie_secondPart!=null){
			String result=value.toString()+splitter+movie_secondPart;
			context.write(new Text(result), NullWritable.get());
		}
	}

}
