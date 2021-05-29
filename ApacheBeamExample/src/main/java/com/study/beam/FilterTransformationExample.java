package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

class CustomerFilter implements SerializableFunction<String ,Boolean>{

	@Override
	public Boolean apply(String input) {
		// TODO Auto-generated method stub
		
           return input.contains("Suganya");
	}
	
	
}
public class FilterTransformationExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> pcollection = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		
		PCollection pcollectionOp = pcollection.apply(Filter.by(new CustomerFilter()));
		
		pcollectionOp.apply(TextIO.write().to("F:\\BeamIP\\FilterOutput.csv"));
		
		pipeline.run();
		
		
	}

}
