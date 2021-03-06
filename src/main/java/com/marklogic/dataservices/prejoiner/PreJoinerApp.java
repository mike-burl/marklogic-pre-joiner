package com.marklogic.dataservices.prejoiner;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.SessionState;
import java.io.*;
import java.util.stream.Stream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.List; 
import org.jdom2.Document;
import org.jdom2.Element;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.jdom2.input.DOMBuilder;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import org.jdom2.output.XMLOutputter;
import java.util.ArrayList;
import java.util.Scanner;


public class PreJoinerApp {
	
	private static final XMLOutputter xmlOutputter = new XMLOutputter();
	
	public static void main(String[] args) throws IOException {
		BlockingQueue<Document> DocQueue = new LinkedBlockingQueue<>(1000);
		String propertyFileLocation = "properties.xml";
		
		Document properties = getProperties(propertyFileLocation);
		
		Element connectionProperties = getConnectionProperties(properties);
		Element csvProperties = getCSVProperties(properties);
		Element entityProperties = getEntityProperties(properties);
		
		createDocumentLoaders(DocQueue, connectionProperties, entityProperties);
		createDocumentProducer(DocQueue, csvProperties);
	}
	
	private static void createDocumentLoaders(BlockingQueue<Document> DocQueue, Element connectionProperties, Element entityProperties) {
		List<Element> hosts = connectionProperties.getChild("hosts").getChildren();
		
		int numThreadsPerHost = Integer.parseInt(connectionProperties.getChildText("numThreadsPerHost"));
		
		ArrayList<DocumentLoader> loaders = new ArrayList<DocumentLoader>();
		for(int i = 0; i < hosts.size(); i++) {
			for(int j = 0; j < numThreadsPerHost; j++) {
				DocumentLoader loader = new DocumentLoader(DocQueue, hosts.get(i).getText(), connectionProperties, entityProperties.clone().detach());
				loaders.add(loader);
				new Thread(loader).start();
			}
		}
		
		return;
	}
	
	private static void createDocumentProducer(BlockingQueue<Document> DocQueue, Element csvProperties) {
		try {
			new Thread(new DocumentProducer(DocQueue, csvProperties)).start();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private static Element getConnectionProperties(Document properties) {
		return properties.getRootElement().getChild("connection");
	}
	
	private static Element getCSVProperties(Document properties) {
		return properties.getRootElement().getChild("data");
	}
	
	private static Element getEntityProperties(Document properties) {
		return properties.getRootElement().getChild("data").getChild("entity");
	}
	
	private static Document getProperties(String propertyFileLocation) {
		Document document = null;
	    try {
	        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	        //If want to make namespace aware.
	        //factory.setNamespaceAware(true);
	        DocumentBuilder documentBuilder = factory.newDocumentBuilder();
	        org.w3c.dom.Document w3cDocument = documentBuilder.parse(propertyFileLocation);
	        document = new DOMBuilder().build(w3cDocument);
	    } catch (IOException | SAXException | ParserConfigurationException e) {
	        e.printStackTrace();
	    }
	    return document;
	}
}