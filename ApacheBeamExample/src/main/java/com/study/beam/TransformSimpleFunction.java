package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class user extends SimpleFunction<String, String>
{  
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String apply(String input)
   {
		String arr[] = input.split(",");
		
		String company = arr[3];
		
		String output = arr[0] + "," + arr[1] + "," + arr[2] + "," + company.toUpperCase();
		
		
	return output;
	   
   }
}

public class TransformSimpleFunction {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> pcollection = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		
		PCollection<String> outputCollection = pcollection.apply(MapElements.via(new user()));
		
		outputCollection.apply(TextIO.write().to("F:\\BeamIP\\Beam.csv").withNumShards(1).withSuffix("csv"));
		
		pipeline.run();
		
		

	}

}
