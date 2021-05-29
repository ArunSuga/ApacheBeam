package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String>  pCollection = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		pCollection.apply(TextIO.write().to("F:\\BeamIP\\Beamop.csv").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();
		
		

	}

}
