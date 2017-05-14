package mapreduce.demo.task4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class Task4Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split(",");
		
		int year = Integer.parseInt(lineArray[0].split("-")[2]);
			
		if (year == 1990) {
			context.write(NullWritable.get(), value);
		}
	}
}
