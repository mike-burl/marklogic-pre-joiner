package com.marklogic.dataservices.prejoiner;

import java.util.concurrent.BlockingQueue;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.SessionState;
import java.io.*;
import java.io.IOException;
import java.util.stream.Stream;
import java.util.ArrayList;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.output.XMLOutputter;
import java.io.StringReader;


public class DocumentLoader implements Runnable {
	private final BlockingQueue<Document> queue;
	private bulkLoaderDS loader;
	//private SessionState ss = new SessionState();
	private DatabaseClient dbclient;
	private final XMLOutputter xmlOutputter = new XMLOutputter();
	private int batchSize;
	private int retryAttempts = 3000;
	private boolean doneRunning = false;
	private int numRetrieved;
	private String exitFlag = "Done";
	ArrayList<Document> collection = new ArrayList<Document>();

    @Override
    public void run() {
        try {
            do {
            	numRetrieved = queue.drainTo(collection, batchSize);
            	if(numRetrieved > 0) {
            		checkCollectionForExitFlag(collection);
            		if(collection.size() > 0) {
	            		try {
	            			load(dbclient, collection);
	            			collection.clear();
	            		} catch (Exception e) {
	            			e.printStackTrace();
	            		}
            		}
            	} else {
            		Thread.sleep(100);
            	}
            } while (!doneRunning);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
		dbclient.release();
    }
    
    private void checkCollectionForExitFlag(ArrayList<Document> collection) throws InterruptedException {
    	for(int i = 0; i < collection.size(); i++) {
    		if(exitFlag.equals(collection.get(i).getRootElement().getName())) {
    			doneRunning = true;
    			queue.put(collection.get(i));
    			collection.remove(i);
    			return;
    		}
    	}
    }
    
    public void load(DatabaseClient dbclient, ArrayList<Document> collection) throws Exception {	
		Reader endpointState = new StringReader("");
		Reader workUnit = new StringReader("");
		Stream<Reader> input = collection.stream().map(doc -> new StringReader(xmlOutputter.outputString(doc)));
		loader.bulkLoadDocs(null, endpointState, workUnit, input);
	}

    public DocumentLoader(BlockingQueue<Document> queue, String host, int batchSize, int port, String username, String password) {
        this.queue = queue;
        this.batchSize = batchSize;
		dbclient = DatabaseClientFactory.newClient(
			host,
			port,
			new DatabaseClientFactory.BasicAuthContext(username, password)
		);
        loader = bulkLoaderDS.on(dbclient);
    }
}
