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

import com.revature.map.MaleMapper;
import com.revature.reduce.PercentOfChange;

public class MaleEmploymentTest {
	private MapDriver<LongWritable, Text,Text, FloatWritable> mapDriver;
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable,Text,FloatWritable> mapReduceDriver;
	
	@Before
	public void setup(){
		MaleMapper mapper = new MaleMapper();
		mapDriver = new MapDriver();
		mapDriver.setMapper(mapper);
		
		PercentOfChange reducer = new PercentOfChange();
		reduceDriver = new ReduceDriver();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"WLD\",\"School enrollment, primary, female (% gross)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"100.0\",\"50.0\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","));
		
		mapDriver.withOutput(new Text("United States") , new FloatWritable(100.0f));
		mapDriver.withOutput(new Text("United States") , new FloatWritable(50.0f));

		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<FloatWritable> values = new ArrayList<>();
		values.add(new FloatWritable(48.478942f));
		values.add(new FloatWritable(46.444118f));
		
		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.withOutput(new Text("United States"), new FloatWritable(-0.12717652f));
		
		reduceDriver.runTest();
	}	

	@Test
	public void testMapReduce(){
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"WLD\",\"School enrollment, primary, female (% gross)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"48.478942\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"46.444118\","));
		
		mapReduceDriver.addOutput(new Text("United States"), new FloatWritable(-0.12717652f));
		
		mapReduceDriver.runTest();
	}
}
