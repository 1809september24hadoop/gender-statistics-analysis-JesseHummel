package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DifferenceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{

	static float[] difference = new float[60];
	static boolean flag = false;

	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)throws IOException, InterruptedException{

		float[] maleNum = new float[45];
		int index = 0;

		for(FloatWritable value: values){
			maleNum[index++] = value.get();
		}	

		context.write(new Text("Difference 1970 - 2014: "), new FloatWritable(maleNum[0] - maleNum[index - 1]));




	}
}
