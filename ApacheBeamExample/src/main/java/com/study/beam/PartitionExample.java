package com.study.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class CompanyPartition implements PartitionFn<String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int partitionFor(String elem, int numPartitions) {
		// TODO Auto-generated method stub
		
		String arr[] = elem.split(",");
		
		if(arr[3].contains("tcs"))
		{
			return 0;
		}else if (arr[3].contains("allsec"))
		{
		return 1;
		}
		
		return 2;
	}
}

public class PartitionExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
        Pipeline pipeline = Pipeline.create();
		
		PCollection<String> pcollection = pipeline.apply(TextIO.read().from("F:\\BeamIP\\Beamip.csv"));
		
		PCollectionList<String> pcollectionlist = pcollection.apply(Partition.of(3, new CompanyPartition()));
		
		pcollectionlist.get(0);
		
//		PCollection<String> outputCollection = pcollection.apply(MapElements.via(new user()));
//		
		pcollectionlist.get(0).apply(TextIO.write().to("F:\\BeamIP\\Partition1.csv").withNumShards(1).withSuffix("csv"));
		
		pcollectionlist.get(1).apply(TextIO.write().to("F:\\BeamIP\\Partition2.csv").withNumShards(1).withSuffix("csv"));
		
		pcollectionlist.get(2).apply(TextIO.write().to("F:\\BeamIP\\Partition3.csv").withNumShards(1).withSuffix("csv"));
		
		pipeline.run();
		

	}

}
