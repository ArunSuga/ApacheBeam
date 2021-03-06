/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.beam.study;

import java.util.ArrayList;
import java.util.Formatter.BigDecimalLayoutForm;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {
	  
	  //System.out.println(args.);
	  
	   MyOptions options =  PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
	   
	   options.setTempLocation("gs://demo-beam-input");
	   options.setStagingLocation("gs://demo-beam-input");
	   options.setProject("demobeam-315009");
	  	  
       Pipeline p = Pipeline.create(options);
    
       List<TableFieldSchema> coloumns = new ArrayList<>();
       
       coloumns.add(new TableFieldSchema().setName("Name").setType("STRING"));
       coloumns.add(new TableFieldSchema().setName("Age").setType("INTEGER"));
       coloumns.add(new TableFieldSchema().setName("EmpID").setType("INTEGER"));
       coloumns.add(new TableFieldSchema().setName("Company").setType("STRING"));
       
       TableSchema tblSchema = new TableSchema().setFields(coloumns);
       
       PCollection<String> pcollection = p.apply(TextIO.read().from("gs://demo-beam-input/Beamip.csv"));
       
       pcollection.apply(ParDo.of(new DoFn<String, TableRow>(){
    	   
    	   @ProcessElement
    	   public void processelements(ProcessContext pc)
    	   {
    		   String[] inparr = pc.element().split(",");
    		   
    		   TableRow row = new TableRow();
    		   row.set("Name", inparr[0]);
    		   row.set("Age" , inparr[1]);
    		   row.set("EmpID", inparr[2]);
    		   row.set("Company", inparr[3]);
    		   pc.output(row);
    		   
    		   
    		   
				/*
				 * if(inparr[3].equals("tcs")) { System.out.println(pc.element()); }
				 */
    		   
    	   }
    	   
       })).apply(BigQueryIO.writeTableRows().to("beam_Study.Employee").withSchema(tblSchema)
    		   .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    		   .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    p.run();
  }
}
