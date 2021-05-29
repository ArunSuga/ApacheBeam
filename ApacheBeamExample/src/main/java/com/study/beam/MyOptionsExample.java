package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class MyOptionsExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<String> pcollection = pipeline.apply(TextIO.read().from(options.getInputFile()));
		
		pcollection.apply(TextIO.write().to(options.getOutputFile()).withNumShards(1).withSuffix(options.getExtension()));
		
		pipeline.run();		

	}

}
