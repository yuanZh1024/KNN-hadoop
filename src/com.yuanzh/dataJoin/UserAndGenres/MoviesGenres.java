package com.yuanzh.dataJoin.UserAndGenres;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MoviesGenres extends Configured implements Tool{
	public static Configuration getMyConfiguration(){
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.app-submission.cross-platform",true);
		conf.set("fs.defaultFS", "hdfs://localhost:9000");// ָ��namenode
		conf.set("yarn.application.classpath","/usr/local/Cellar/hadoop/3.1.1/libexec/etc/hadoop:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/common/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/common/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/hdfs/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/mapreduce/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/mapreduce/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn/lib/*:/usr/local/Cellar/hadoop/3.1.1/libexec/share/hadoop/yarn/*");
		conf.set("mapreduce.framework.name","yarn"); // ָ��ʹ��yarn���
		String resourcenode="localhost";
		conf.set("yarn.resourcemanager.address", resourcenode+":8032"); // ָ��resourcemanager
		conf.set("yarn.resourcemanager.scheduler.address",resourcenode+":8030");// ָ����Դ������
		conf.set("mapreduce.jobhistory.address",resourcenode+":10020");
		conf.set("mapreduce.job.jar",JarUtil.jar(MoviesGenres.class));
		return conf;	
	}
	public static void main(String[] args) {
		String[] myArgs={
				"/movie/users_movies/part-m-00000",
				"/movie/gender_genre",
				"::"
		};
		try {
			ToolRunner.run(getMyConfiguration(), new MoviesGenres(), myArgs);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=3){
			System.err.println("MoviesGenres <input> <output> <splitter>");
			System.exit(-1);
		}
		Configuration conf=getMyConfiguration();
		conf.set("SPLITTER", args[2]);
		Job job=Job.getInstance(conf, "movies_genres");
		job.setJarByClass(MoviesGenres.class);
		job.setMapperClass(MoviesGenresMapper.class);
		job.setReducerClass(MoviesGenresReducer.class);
		job.setMapOutputKeyClass(UserAndGender.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem.get(conf).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?-1:1;
	}

}
