package com.marklogic.dataservices.prejoiner;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.SessionState;
import java.io.*;
import java.io.IOException;
import java.util.stream.Stream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.jdom2.Document;
import org.jdom2.Element;
import org.supercsv.prefs.CsvPreference;
import java.util.ArrayList;
import java.util.List;
import java.lang.StringBuilder;

public class DocumentProducer implements Runnable {
	private final BlockingQueue<Document> DocQueue;
	
	private Element csvProperties;
	
	boolean stillProcessing = true;
	String exitFlag = "Done";
	String[] primaryKeyIDs;
	String primaryName;
	String entity;
	
	private Element primaryElement;
	private ArrayList<CSVRowConsumer> csvConsumers = new ArrayList<CSVRowConsumer>();

	private final CSVRowConsumer primaryConsumer;

	@Override
    public void run() {
		Document put;
        try {
        	while(stillProcessing) {
        		primaryElement = primaryConsumer.getNextElement();
        		if(!exitFlag.equals(primaryElement.getName())) {
        			put = getRelatedElements(primaryElement);
        			this.DocQueue.put(put);
        		} else {
        			System.out.println("[DocumentProducer] Done!");
        			stillProcessing = false;
        		}
        	}
        	
            Element doneElement = new Element("Done");
            this.DocQueue.put(new Document().setContent(doneElement));
            
        } catch (Exception e) {
        	e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
	
	private Document getRelatedElements(Element primaryElement) throws InterruptedException {
		StringBuilder sb = new StringBuilder("");
		for(int i = 0; i < primaryKeyIDs.length; i++) {
			sb.append(primaryElement.getChild(this.primaryName).getChildText(primaryKeyIDs[i]).trim());
		}
		String primaryKey = sb.toString();
		
		Element envelope = new Element("envelope");
		Element tempChildElement;
		
		envelope.addContent(primaryElement);
		
		for(int i = 0; i < csvConsumers.size(); i++) {
			tempChildElement = csvConsumers.get(i).findChildOffQueue(primaryKey);
			if(null != tempChildElement && !exitFlag.equals(tempChildElement.getName())) {
				envelope.addContent(tempChildElement);
			}
		}

		return new Document().setContent(envelope);
	}
	
	private static CsvPreference getCsvPreference(String preference) {
		if(preference.equals("STANDARD_PREFERENCE")) return CsvPreference.STANDARD_PREFERENCE;
		else if(preference.equals("EXCEL_PREFERENCE")) return CsvPreference.EXCEL_PREFERENCE;
		else if(preference.equals("EXCEL_NORTH_EUROPE_PREFERENCE")) return CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE;
		else if(preference.equals("TAB_PREFERENCE")) return CsvPreference.TAB_PREFERENCE;
		else return null;
	}
	
	public DocumentProducer(BlockingQueue<Document> DocQueue, Element csvProperties) throws InterruptedException {
        this.DocQueue = DocQueue;
        this.csvProperties = csvProperties;
        this.entity = csvProperties.getChildText("entity");
        
        Element primaryCSV = csvProperties.getChild("primaryCSV");
        this.primaryName = primaryCSV.getChildText("name");
        this.primaryKeyIDs = primaryCSV.getChildText("primaryKey").split(",");
        
        CsvPreference cp = getCsvPreference(csvProperties.getChildText("parser"));
        
        primaryConsumer = new CSVRowConsumer(
        		cp,
        		primaryCSV.getChildText("location"), 
        		primaryCSV.getChildText("header"), 
        		primaryCSV.getChildText("primaryKey"), 
        		this.primaryName
        );
                
        List<Element> csvConsumerProperties = csvProperties.getChild("childCSVs").getChildren("childCSV");
        for(int i = 0; i < csvConsumerProperties.size(); i++) {
        	this.csvConsumers.add(
        			new CSVRowConsumer(
        					cp,
        					csvConsumerProperties.get(i).getChildText("location"),
        					csvConsumerProperties.get(i).getChildText("header"),
        					csvConsumerProperties.get(i).getChildText("primaryKey"),
        					csvConsumerProperties.get(i).getChildText("name")
        			)
        	);
        }
    }
}