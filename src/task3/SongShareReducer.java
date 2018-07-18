package task3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SongShareReducer extends Reducer<Text,IntWritable,Text, IntWritable> {
	
	public void reducer(Text uid, Iterable<IntWritable> count, Context context) throws IOException, InterruptedException{
		
		int sum = 0;
		for(IntWritable value:count){
			sum+= value.get();
		}
		context.write(uid, new IntWritable(sum));
	}

}
