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


public class AccountStreamer {
	public static final String PUBSUB_TOPIC = "projects/<project-id>/topics/accounts";
	public static final String BIGQUERY_DATASET = "<project-id>:<dataset>.accounts";

	static class FormatAsTableRowFn extends DoFn<TableRow, TableRow> {
		private static final long serialVersionUID = 0;
	    private static final Logger LOG = LoggerFactory.getLogger(AccountStreamer.class);

		static TableSchema getSchema() {
			return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
				// Compose the list of TableFieldSchema from tableSchema.
				{
					add(new TableFieldSchema().setName("AccountID").setType("STRING").setMode("REQUIRED"));
					add(new TableFieldSchema().setName("Code").setType("STRING").setMode("REQUIRED"));
					add(new TableFieldSchema().setName("Name").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("Status").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("Type").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("TaxType").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("Class").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("EnablePaymentsToAccount").setType("BOOLEAN").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("ShowInExpenseClaims").setType("BOOLEAN").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("BankAccountNumber").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("BankAccountType").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("CurrencyCode").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("ReportingCode").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("ReportingCodeName").setType("STRING").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("HasAttachments").setType("BOOLEAN").setMode("NULLABLE"));
					add(new TableFieldSchema().setName("UpdatedDateUTC").setType("DATE").setMode("NULLABLE"));

				}
			});
		}

		@Override
		public void processElement(ProcessContext c) throws IOException {
			TableRow accountsObj = c.element();
			
			LOG.info("Original input:" + c.element().toPrettyString());
			
			ArrayList<Map> accountRows = (ArrayList<Map>) accountsObj.get("Accounts");
			Iterator<Map> accountsIterator = accountRows.iterator();

			while (accountsIterator.hasNext()) {

				Map accountRow = (Map) accountsIterator.next();
                LOG.info("Child input JSON: " + accountRow.toString());
            
				TableRow bQueryRow = new TableRow().set("AccountID", (String) accountRow.get("AccountID"))
						.set("Code", (String) accountRow.get("Code").toString())
						.set("Name", (String) accountRow.get("Name"))
						.set("Status", (String) accountRow.get("Status"))
						.set("Type", (String) accountRow.get("Type"))
						.set("TaxType", (String) accountRow.get("TaxType"))
						.set("Class", (String) accountRow.get("Class"))
						.set("EnablePaymentsToAccount", (Boolean) accountRow.get("EnablePaymentsToAccount"))
						.set("ShowInExpenseClaims", (Boolean) accountRow.get("ShowInExpenseClaims"))
						.set("BankAccountType", (String) accountRow.get("BankAccountType"))
						.set("CurrencyCode", (String) accountRow.get("CurrencyCode"))
						.set("ReportingCode", (String) accountRow.get("ReportingCode"))
						.set("ReportingCodeName", (String) accountRow.get("ReportingCodeName"))
						.set("HasAttachments", (Boolean) accountRow.get("HasAttachments"));						
				
				c.output(bQueryRow);
                LOG.info("Write to BigQuery success");
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
