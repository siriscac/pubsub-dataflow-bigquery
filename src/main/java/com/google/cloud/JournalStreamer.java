package com.google.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JournalStreamer {
	public static final String PUBSUB_TOPIC = "projects/<project-id>/topics/journals";
	public static final String BIGQUERY_DATASET = "<projects>:<dataset>.journal_lines";

	static class FormatAsTableRowFn extends DoFn<TableRow, TableRow> {
		private static final long serialVersionUID = 0;
	    private static final Logger LOG = LoggerFactory.getLogger(JournalStreamer.class);

		static TableSchema getSchema() {
			return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
				// Compose the list of TableFieldSchema from tableSchema.
				{
					add(new TableFieldSchema().setName("JournalLineID").setType("STRING").setMode("REQUIRED"));
					add(new TableFieldSchema().setName("JournalID").setType("STRING").setMode("REQUIRED"));
					add(new TableFieldSchema().setName("AccountID").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("AccountCode").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("AccountType").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("AccountName").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("NetAmount").setType("FLOAT").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("GrossAmount").setType("FLOAT").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("TaxAmount").setType("FLOAT").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("TaxType").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("TaxName").setType("STRING").setMode("NULLABLE"));
				}
			});
		}

		@Override
		public void processElement(ProcessContext c) throws IOException {
			TableRow journalsObj = c.element();
			
			LOG.info("Original input:" + c.element().toPrettyString());
			
			ArrayList<Map> journalRows = (ArrayList<Map>) journalsObj.get("Journals");
			Iterator<Map> journalsIterator = journalRows.iterator();

			while (journalsIterator.hasNext()) {

				Map journalRow = (Map) journalsIterator.next();
                LOG.info("Child input JSON: " + journalRow.toString());

                
				String journalId = (String) journalRow.get("JournalID");
				//LinkedHashMap journalLineObj = (LinkedHashMap) journalRow.get("JournalLines");
				ArrayList<Map> journalLineRows = (ArrayList<Map>) journalRow.get("JournalLines");
				
				Iterator<Map> journalLinesIterator = journalLineRows.iterator();
				
				while (journalLinesIterator.hasNext()) {			
					Map journalLineRow = (Map) journalLinesIterator.next();
					Double taxAmount = 0.0;
					if(journalLineRow.get("TaxAmount").toString() == "0"){
						taxAmount = ((Integer)journalLineRow.get("TaxAmount") * 1.0);
					} else {
						taxAmount = (Double) journalLineRow.get("TaxAmount");
					}
					TableRow bQueryRow = new TableRow().set("JournalLineID", (String) journalLineRow.get("JournalLineID"))
							.set("JournalID", journalId)
							.set("AccountID", (String) journalLineRow.get("AccountID"))
							.set("AccountCode", (String) journalLineRow.get("AccountCode").toString())
							.set("AccountType", (String) journalLineRow.get("AccountType"))
							.set("AccountName", (String) journalLineRow.get("AccountName"))
							.set("NetAmount", (Double) journalLineRow.get("NetAmount"))
							.set("GrossAmount", (Double) journalLineRow.get("GrossAmount"))
							.set("TaxAmount", (Double) taxAmount)
							.set("TaxType", (String) journalLineRow.get("TaxType"))
							.set("TaxName", (String) journalLineRow.get("TaxName"));
					
					c.output(bQueryRow);
	                LOG.info("Write to BigQuery success");
				}
			}
		}
	}

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		options.as(DataflowPipelineOptions.class).setStreaming(true);
		Pipeline p = Pipeline.create(options);

		p.apply(PubsubIO.Read.topic(PUBSUB_TOPIC).withCoder(TableRowJsonCoder.of()))
				.apply(ParDo.of(new FormatAsTableRowFn()))
				.apply(BigQueryIO.Write.to(BIGQUERY_DATASET).withSchema(FormatAsTableRowFn.getSchema()).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

		p.run();
	}

}
