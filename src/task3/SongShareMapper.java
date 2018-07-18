package task3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class SongShareMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] line = value.toString().split("\\|");
		if(line[2].equals("1")){
			context.write(new Text(line[0]), new IntWritable(1));
		}
	}

}
