package com.study.beam;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class SideInputExample {
	
	public static void main(String[] args) {
		
		Pipeline pipeline = Pipeline.create();
		
	    PCollection<KV<String,String>> pReturn =  pipeline.apply(TextIO.read().from("F:\\BeamIP\\BeamReturn.csv"))
	    .apply(ParDo.of(new DoFn<String,KV<String,String>>(){
	    	
	    	@ProcessElement
	    	public void ProcessElement(ProcessContext pc)
	    	{
	    		String[] arr = pc.element().split(",");
	    		
	    		pc.output(KV.of(arr[0], arr[1]));
	    	}
	    	
	    }));
	  
	  	PCollectionView<Map<String,String>> pMap = pReturn.apply(View.asMap());

		PCollection<String> pCollection = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		
		pCollection.apply(ParDo.of(new DoFn<String , String>(){
			
			@ProcessElement
			public void processElement(ProcessContext pc)
			{
				Map<String,String> sideInputMap = pc.sideInput(pMap);
				
				String[] arr = pc.element().split(",");
				
				String name = sideInputMap.get(arr[0]);
				
				//System.out.println(pc.element());
				
				if(name == null)
				{
					System.out.println(pc.element());
				}
			}
			
		}).withSideInputs(pMap));
		
		pipeline.run();

	}
	
}
