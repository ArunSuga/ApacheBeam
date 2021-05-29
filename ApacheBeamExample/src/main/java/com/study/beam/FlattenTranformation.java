package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenTranformation {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> pcollection1 = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		
		PCollection<String> pcollection2 = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip1.csv"));
		
 		PCollectionList<String> pcollectionList  = PCollectionList.of(pcollection1).and(pcollection2);
 		
 		PCollection<String> mergedList = pcollectionList.apply(Flatten.pCollections());
 		
 		mergedList.apply(TextIO.write().to("F:\\BeamIP\\Merged.csv").withNumShards(1));
 		
 		pipeline.run();
	}

}
