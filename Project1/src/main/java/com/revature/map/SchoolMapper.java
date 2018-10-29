package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SchoolMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{

		String line = value.toString().substring(1);
		String[] code = line.split("\",\"");
		String num = "";
		float percent = 0.0f;

		int index = 4;
		for(;index < code.length; index++){

			if(code[1].equals("WLD") && code[3].equals("SE.SCH.LIFE.MA")&& !code[index].equals("")){
				if(code[index].length() >= 2 && code[index].substring(code[index].length() - 2).equals("\",")){
					num = code[index].substring(0, code[index].length() - 2);
				}else{
					num = code[index];
				}
				if(!num.equals("")){
					percent = Float.parseFloat(num);
					context.write(new Text("Male"), new FloatWritable(Float.parseFloat(code[index])));
				}
			}

		}

	}

}
