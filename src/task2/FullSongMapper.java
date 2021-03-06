package task2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class FullSongMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] line = value.toString().split("\\|");
		Text uid = new Text(line[0]);
		if(line[4].equals("1")){
			context.write(uid,new IntWritable(1));
		}
	}
}
