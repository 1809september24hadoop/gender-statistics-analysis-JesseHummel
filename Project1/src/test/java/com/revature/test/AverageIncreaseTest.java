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

import com.revature.map.GraduateMapper;
import com.revature.reduce.AverageReducer;

public class AverageIncreaseTest {
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	private ReduceDriver<Text,FloatWritable, Text, FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text,FloatWritable, Text,FloatWritable> mapReduceDriver;
	
	@Before
	public void setup(){
		GraduateMapper mapper = new GraduateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		AverageReducer reducer = new AverageReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper(){
		
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"School enrollment, primary, female (% gross)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10.0\",\"10.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		mapDriver.addOutput(new Text("United States"), new FloatWritable(10.0f));
		mapDriver.addOutput(new Text("United States"), new FloatWritable(10.0f));
		
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		
		List<FloatWritable> values = new ArrayList<>();
		values.add(new FloatWritable(100.00f));
		values.add(new FloatWritable(200.00f));
		
		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.withOutput(new Text("United States"), new FloatWritable(50.00f));
		
		reduceDriver.runTest();	
	}
	
	@Test
	public void testMapReduce(){
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"School enrollment, primary, female (% gross)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"100.0\",\"50.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		mapReduceDriver.addOutput(new Text("United States"), new FloatWritable(-25.0f));
		
		mapReduceDriver.runTest();
	}

}
