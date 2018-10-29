package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PercentOfChange extends Reducer<Text, FloatWritable,Text,FloatWritable>{

	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
		
		int index = 0;
		float average = 0.0f;
		float[] percents = new float[30];
		
		for(FloatWritable value: values){
			percents[index++] = value.get();
		}
		
		average = (percents[index - 1] - percents[0]) / 16;
		context.write(new Text(key), new FloatWritable(average));
	}
	
}
