package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class custfilter extends DoFn<String, String>{
	
	private static final long serialVersionUID = 1L;
	
	@ProcessElement
	public void ProcessElement(ProcessContext c)
	{
		String line = c.element();
		
		String arr[] = line.split(",");
		
		if (arr[3].equalsIgnoreCase("TCS")) {
			
			c.output(line);
		}
	}
}

public class PardoTransformation {
	
	public static void main(String[] args) {
		  
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> pcollection = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		
		PCollection<String> outputCollection = pcollection.apply(ParDo.of(new custfilter()));
		
//		PCollection<String> outputCollection = pcollection.apply(MapElements.via(new user()));
//		
	    outputCollection.apply(TextIO.write().to("F:\\BeamIP\\Beampardo.csv").withHeader("Name,Age,ID,Company").withNumShards(1).withSuffix("csv"));
//		
		pipeline.run();
		
	}

}
