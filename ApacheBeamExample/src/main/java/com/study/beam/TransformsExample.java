package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class TransformsExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> custlist = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		
		//TypeDescriptors example
		
		PCollection<String> outputlist =custlist.apply(MapElements.into(TypeDescriptors.strings()).via((String obj) -> obj.toUpperCase()));
		
		outputlist.apply(TextIO.write().to("F:\\BeamIP\\TransformOutput.csv"));
		
		pipeline.run();
	}

}
