package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.SchoolMapper;
import com.revature.reduce.DifferenceReducer;

public class DifferenceInSchool {
	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.printf("Usage: FemaleGraduates <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(DifferenceInSchool.class);
		
		job.setJobName("School Difference");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));;
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SchoolMapper.class);

		job.setReducerClass(DifferenceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}
}
