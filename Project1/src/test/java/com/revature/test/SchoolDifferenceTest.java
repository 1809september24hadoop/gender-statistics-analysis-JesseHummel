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

import com.revature.map.SchoolMapper;
import com.revature.reduce.DifferenceReducer;

public class SchoolDifferenceTest {
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	private ReduceDriver<Text,FloatWritable, Text, FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text,FloatWritable, Text,FloatWritable> mapReduceDriver;
	
	@Before
	public void setup(){
		SchoolMapper mapper = new SchoolMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		DifferenceReducer reducer = new DifferenceReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper(){
		
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"WLD\",\"School enrollment, primary, female (% gross)\",\"SE.SCH.LIFE.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10.0\",\"10.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));


		mapDriver.addOutput(new Text("Male"), new FloatWritable(10.0f));
		mapDriver.addOutput(new Text("Male"), new FloatWritable(10.0f));		
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		
		List<FloatWritable> values = new ArrayList<>();
		values.add(new FloatWritable(200.00f));
		values.add(new FloatWritable(100.00f));
		
		reduceDriver.withInput(new Text("Male"), values);
		reduceDriver.withOutput(new Text("Difference 1970 - 2014: "), new FloatWritable(100.00f));
		
		reduceDriver.runTest();	
	}
	
	@Test
	public void testMapReduce(){
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"WLD\",\"School enrollment, primary, female (% gross)\",\"SE.SCH.LIFE.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"100.0\",\"50.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		mapReduceDriver.addOutput(new Text("Difference 1970 - 2014: "), new FloatWritable(50.0f));
		
		mapReduceDriver.runTest();
	}

}
