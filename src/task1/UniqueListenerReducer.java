package task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueListenerReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	public void reduce(Text uid, Iterable<IntWritable> count, Context context)
					throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable values: count){
			sum+= values.get();			
		}
		context.write(uid,new IntWritable(sum));
	}
}
