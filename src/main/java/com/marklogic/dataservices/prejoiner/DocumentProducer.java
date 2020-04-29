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
	private ArrayList<DocumentLoader> loaders;

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
        } catch (Exception e) {
        	e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        
        for(int i = 0; i < loaders.size(); i++) {
        	loaders.get(i).doneRunning();
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
	
	public DocumentProducer(BlockingQueue<Document> DocQueue, Element csvProperties, ArrayList<DocumentLoader> loaders) throws InterruptedException {
        this.DocQueue = DocQueue;
        this.csvProperties = csvProperties;
        this.loaders = loaders;
        this.entity = csvProperties.getChildText("entity");
        
        Element primaryCSV = csvProperties.getChild("primaryCSV");
        this.primaryName = primaryCSV.getChildText("name");
        this.primaryKeyIDs = primaryCSV.getChildText("primaryKey").split(",");
        
        primaryConsumer = new CSVRowConsumer(
        		primaryCSV.getChildText("location"), 
        		primaryCSV.getChildText("header"), 
        		primaryCSV.getChildText("primaryKey"), 
        		this.primaryName,
        		primaryCSV.getChildText("separator")
        );
        
        List<Element> csvConsumerProperties = csvProperties.getChild("childCSVs").getChildren("childCSV");
        for(int i = 0; i < csvConsumerProperties.size(); i++) {
        	this.csvConsumers.add(
        			new CSVRowConsumer(
        					csvConsumerProperties.get(i).getChildText("location"), 
        					csvConsumerProperties.get(i).getChildText("header"), 
        					csvConsumerProperties.get(i).getChildText("primaryKey"), 
        					csvConsumerProperties.get(i).getChildText("name"),
        					csvConsumerProperties.get(i).getChildText("separator")
        			)
        	);
        }
    }
}