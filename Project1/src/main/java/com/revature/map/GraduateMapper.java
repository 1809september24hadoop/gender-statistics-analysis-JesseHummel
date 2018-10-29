package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraduateMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String num = "";
		String line = value.toString().substring(1);
		String[] code = line.split("\",\"");
		//\\W+ all white space  CODE SE.PRM.CMPL.FE.ZS
		float percent = 0.0F;
		int index = 44;
		for(;index < code.length;index++){
			if(code[1].equals("USA") && code[3].equals("SE.PRM.NENR.FE")&& !code[index].equals("")){
				if(code[index].length() >= 2 && code[index].substring(code[index].length() - 2).equals("\",")){
					num = code[index].substring(0, code[index].length() - 2);
				}else{
				num = code[index];
				}
				if(!num.equals("")){
				percent = Float.parseFloat(num);
				context.write(new Text(code[0]), new FloatWritable(percent));
				}
			}
		}
		
	}
}
