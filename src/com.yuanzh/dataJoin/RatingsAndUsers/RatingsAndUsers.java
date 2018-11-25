package com.yuanzh.dataJoin.RatingsAndUsers;
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
import sun.security.krb5.Config;

import java.net.URI;

public class RatingsAndUsers extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=4){
			System.err.println("com.yuanzh.dataJoin.RatingsAndUsers <cachePath> <input> <output> <splitter>");
			System.exit(-1);
		}
		Configuration conf=RatingsAndUsers.getMyConfiguration();
		//DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
		conf.set("SPLITTER", args[3]);
		Job job=Job.getInstance(conf, "joindata");
		URI[] path = new URI[1];
		path[0] = new Path(args[0]).toUri();
		job.setCacheFiles(path);

		job.setJarByClass(RatingsAndUsers.class);
		job.setMapperClass(JoinMapper.class);
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
				"/movie/users.dat",
				"/movie/ratings.dat",
				"/movie/ratings_users",
				"::"
		};
		try {
			ToolRunner.run(getMyConfiguration(), new RatingsAndUsers(), myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static Configuration getMyConfiguration(){
		Configuration conf = new Configuration();
		conf.set("yarn.application.classpath","/usr/local/Cellar/hadoop/3.1.1/libexec/etc/hadoop:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/common/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/common/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/mapreduce/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/mapreduce/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn/*");
		conf.setBoolean("mapreduce.app-submission.cross-platform",true);
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("mapreduce.framework.name","yarn");
		String resourcenode="localhost";
		conf.set("yarn.resourcemanager.address", resourcenode+":8032");
		conf.set("yarn.resourcemanager.scheduler.address",resourcenode+":8030");
		conf.set("mapreduce.jobhistory.address",resourcenode+":10020");
		conf.set("mapreduce.job.jar",JarUtil.jar(RatingsAndUsers.class));
		return conf;	
	}
	

}
