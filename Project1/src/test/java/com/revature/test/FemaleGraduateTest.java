package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.AnalysisMapper;
import com.revature.reduce.LessThan30Reducer;

public class FemaleGraduateTest {
	
	private MapDriver<LongWritable, Text,Text, FloatWritable> mapDriver;
	private ReduceDriver<Text,FloatWritable,Text,FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable,Text,FloatWritable> mapReduceDriver;
	
	@Before
	public void setup(){
		AnalysisMapper mapper = new AnalysisMapper();
		mapDriver = new MapDriver();
		mapDriver.setMapper(mapper);
		
		LessThan30Reducer reducer = new LessThan30Reducer();
		reduceDriver = new ReduceDriver();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(1), new Text("\"Afghanistan\",\"AFG\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"45.02818\",\"\",\"17.07411\","));
		
		mapDriver.withOutput(new Text("Afghanistan") , new FloatWritable(45.02818f));
		mapDriver.withOutput(new Text("Afghanistan") , new FloatWritable(17.07411f));

		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<FloatWritable> values = new ArrayList<>();
		values.add(new FloatWritable(33.023f));
		values.add(new FloatWritable(22.313f));
		
		reduceDriver.withInput(new Text("Afghanistan"), values);
		reduceDriver.withOutput(new Text("Afghanistan"), new FloatWritable(22.313f));
		
		reduceDriver.runTest();
	}	

	@Test
	public void testMapReduce(){
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Afghanistan\",\"AFG\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"45.02818\",\"\",\"17.07411\","));
		
		mapReduceDriver.addOutput(new Text("Afghanistan"), new FloatWritable(17.07411f));
		
		mapReduceDriver.runTest();
	}

}
