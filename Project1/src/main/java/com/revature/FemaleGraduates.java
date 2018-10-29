package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.AnalysisMapper;
import com.revature.reduce.LessThan30Reducer;



public class FemaleGraduates {
	
	public static void main(String[] args) throws Exception{
		
		
		if(args.length != 2){
			System.out.printf("Usage: FemaleGraduates <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(FemaleGraduates.class);
		
		job.setJobName("Female Graduates");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));;
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AnalysisMapper.class);
				
		//job.setCombinerClass(SumReducer.class);
		
		//Split/Sort face, Partitioner INVESTIGATE :D
		job.setReducerClass(LessThan30Reducer.class);
		//job.setReducerClass(SumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}

}
