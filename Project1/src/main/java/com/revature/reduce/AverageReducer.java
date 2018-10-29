package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, FloatWritable, Text,FloatWritable> {

	@Override
	public void reduce(Text key, Iterable<FloatWritable> values,Context context)
			throws IOException, InterruptedException{
		
		float[] yearPercent = new float[17];
		int index = 0;
		int count = 0;
		for(FloatWritable value: values){
			yearPercent[index++] = value.get();
			count++;
		}
		float average = 0.0f;
		for(int i = 0; i < count - 1;i++){
			average += (yearPercent[i+1] - yearPercent[i]);
		}
		
		average = average / count;
		
		context.write(new Text(key), new FloatWritable(average));
	}
}
