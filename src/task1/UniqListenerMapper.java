package task1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class UniqListenerMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	public void map(LongWritable key, Text values, Context context)
				throws IOException,InterruptedException{
		
		String[] line = values.toString().split("\\|");
		Text uid = new Text(line[0]);
		IntWritable value = new IntWritable(1);
		context.write(uid,value);
	}
}