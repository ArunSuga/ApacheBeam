package com.study.beam;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
	
	void setInputFile(String file);
	
	String getInputFile();
	
	void setOutputFile(String file);
	
	String getOutputFile();
	
	void setExtension(String extn);
	
	String getExtension();

}
