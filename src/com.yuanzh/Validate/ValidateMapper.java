package com.yuanzh.Validate;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class ValidateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String splitter="";
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		splitter=context.getConfiguration().get("SPLITTER");
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] val=value.toString().split(splitter);
		//val[0]是预测类别，val[2]是实际类别
		context.write(NullWritable.get(), new Text(val[0]+splitter+val[2]));
	}
}
