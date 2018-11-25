package com.yuanzh.dataJoin.UsersAndMovies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


public class UsersAndMovies extends Configured implements Tool {
	public static Configuration getMyConfiguration(){
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.app-submission.cross-platform",true);
		conf.set("fs.defaultFS", "hdfs://localhost:9000");// ָ��namenode
		conf.set("mapreduce.framework.name","yarn"); // ָ��ʹ��yarn���
		String resourcenode="localhost";
		conf.set("yarn.resourcemanager.address", resourcenode+":8032"); // ָ��resourcemanager
		conf.set("yarn.resourcemanager.scheduler.address",resourcenode+":8030");// ָ����Դ������
		conf.set("mapreduce.jobhistory.address",resourcenode+":10020");
		conf.set("mapreduce.job.jar",JarUtil.jar(UsersAndMovies.class));
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=4){
			System.err.println("demo.RatingsAndUsers <cachePath> <input> <output> <splitter>");
			System.exit(-1);
		}
		Configuration conf=UsersAndMovies.getMyConfiguration();
		conf.set("yarn.application.classpath","/usr/local/Cellar/hadoop/3.1.1/libexec/etc/hadoop:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/common/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/common/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/mapreduce/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/mapreduce/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn/*");
		//DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
		conf.set("SPLITTER", args[3]);
		Job job=Job.getInstance(conf, "joindata");

		URI[] paths = new URI[1];
		paths[0]=new URI(args[0]);
		job.setCacheFiles(paths);

		job.setJarByClass(UsersAndMovies.class);
		job.setMapperClass(UsersMoviesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[2]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?-1:1;
	}
	public static void main(String[] args) {
		String[] myArgs={
				"/movie/movies.dat",
				"/movie/ratings_users/part-m-00000",
				"/movie/users_movies",
				"::"
		};
		try {
			ToolRunner.run(getMyConfiguration(), new UsersAndMovies(), myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
