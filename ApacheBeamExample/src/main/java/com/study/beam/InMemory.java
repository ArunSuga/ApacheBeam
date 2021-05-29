package com.study.beam;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class InMemory {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Pipeline pipeline = Pipeline.create();
		PCollection<CustomerEntity>  plist = pipeline.apply(Create.of(getCustomers()));
		
		PCollection<String> newPList = plist.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity cust) -> cust.getName()));
		newPList.apply(TextIO.write().to("F:\\BeamIP\\Cutomer.csv").withNumShards(1).withSuffix("csv"));
		
		pipeline.run();

	}
	
	static List<CustomerEntity>  getCustomers()
	{
		List<CustomerEntity> customers = new ArrayList();
		
		CustomerEntity ent1 = new CustomerEntity("01" ,"Suganya");
		CustomerEntity ent2 = new CustomerEntity("02" ,"Arun");
		
		customers.add(ent1);
		customers.add(ent2);
		
		return customers;
		
	}

}
