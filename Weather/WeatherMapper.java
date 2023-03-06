package com.Weather;
import java.io.IOException;
import java.util.Iterator;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
	

public  class WeatherMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	

	@Override
	public void map(LongWritable arg0, Text Value, Context context)
			throws IOException, InterruptedException {

	//Converting the record (single line) to String and storing it in a String variable line
		
	String line = Value.toString();
		
	//Checking if the line is not empty
		
		if (!(line.length() == 0))
		{
			
	    String date = line.substring(6, 14);              //date

		float temp_Max = Float.parseFloat(line.substring(39, 45).trim());  
			
		float temp_Min = Float.parseFloat(line.substring(47, 53).trim());  

		//if maximum temperature is greater than 35 , its a hot day
			
	if (temp_Max > 35.0) {
			
		context.write(new Text("Hot Day " + date),new Text(String.valueOf(temp_Max))); 	// Hot day

        				}

			//if minimum temperature is less than 10 , its a cold day
			
	if (temp_Min < 10) {
			
context.write(new Text("Cold Day " + date),new Text(String.valueOf(temp_Min)));    	// Cold day
		            	}
		}
	}

}