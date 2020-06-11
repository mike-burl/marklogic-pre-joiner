package com.marklogic.dataservices.prejoiner;

import java.util.concurrent.BlockingQueue;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import org.jdom2.Element;
import org.jdom2.output.XMLOutputter;
import java.lang.StringBuilder;
import java.lang.reflect.Field;
import java.util.regex.Pattern;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.CsvMapReader;

public class CSVRowProducer implements Runnable {
	private final BlockingQueue<Element> CSVQueue;
	private final XMLOutputter xmlOutputter = new XMLOutputter();
	
	String CSVFileName;
	String CurrentRowPK;
	String[] PrimaryKeyColumns;
	String Header;
	String EntityName;
	String[] CSVHeaders;
	String CSVSeparatorChar = ",";
	boolean bufferFull = false;
	boolean collectionReady = false;
	boolean doneProcessing = false;
	
	int[] pkIndexPositions;
	int sleepTime = 0;
	
	ICsvMapReader CSVFileReader;
	Element CurrentCollection;
	Element PutCollection;
	Element DoneElement = new Element("Done");

	@Override
    public void run() {
        try {
            process();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
	
	private void process() throws InterruptedException {
		Map<String,String> CSVLine;
		try {
			while (!doneProcessing) {
				CSVLine = CSVFileReader.read(CSVHeaders);
				if(null != CSVLine) {
					generateXML(CSVLine);
				} else {
					PutCollection = CurrentCollection;
					doneProcessing = true;
				}
			}
			// No rows left to ingest, however we still need to push the final element we made
			// As well as the signal to the orchestrator that work has finished
			CSVQueue.put(PutCollection);
			CSVQueue.put(DoneElement);
		} catch (IOException e) {
            e.printStackTrace();
        }
		System.out.println("[CSVRowProducer] " + CSVFileName + " done!");
    }
	
	public void generateXML(Map<String,String> CSVLine) throws InterruptedException {
		// First, check the primary key.  
		// If CurrentRowPK is null then we want to start a new element and add the row to it
		// If CurrentRowPK is the same as the primary key, add the row to the element
		// If CurrentRowPK is different, flag the element as being ready to add to the queue and start a new element
		//String[] ElementValues = CSVLine.split(CSVSeparatorChar, -1);

		StringBuilder sb = new StringBuilder("");
		for(int i = 0; i < PrimaryKeyColumns.length; i++) {
			sb.append(CSVLine.get(PrimaryKeyColumns[i]));
		}
		String PK = sb.toString();
		
		if(null == CurrentRowPK) {
			System.out.println("[RowProducer] Starting new CSV file with primary key : " + PK);
			CurrentCollection = new Element(EntityName + "_Collection");
			CurrentRowPK = PK;
			addChildRow(CSVLine, PK);
		} else if(PK.equals(CurrentRowPK)) {
			addChildRow(CSVLine, PK);
		} else {
			PutCollection = CurrentCollection;
			CSVQueue.put(PutCollection);
			CurrentCollection = new Element(EntityName + "_Collection");
			CurrentRowPK = PK;
			addChildRow(CSVLine, PK);
		}
	}
	
	public void addChildRow(Map<String,String> CSVLine, String PK) {
		Element row = new Element(EntityName);
        
        for(int i = 0; i < CSVHeaders.length; i++){
        	try {
        		row.addContent(new Element(CSVHeaders[i]).setText(CSVLine.get(CSVHeaders[i])));
        	} catch (ArrayIndexOutOfBoundsException e) {
        		System.out.println("[CSVRowProducer] Error in file : " + CSVFileName);
        		System.out.println("[CSVRowProducer] Could not parse line : " + PK);
        	}
        }
        
        CurrentCollection.addContent(row);
	}
	
	public CSVRowProducer(CsvPreference cp, BlockingQueue<Element> CSVQueue, String CSVFileName, String Header, String[] PrimaryKeyColumns, String EntityName, String separator) {
        this.CSVQueue = CSVQueue;
        this.PrimaryKeyColumns = PrimaryKeyColumns;
        this.Header = Header;
        this.EntityName = EntityName;
        this.CSVHeaders = Header.split(",");
        this.CSVSeparatorChar = Pattern.quote(separator);
        
        this.pkIndexPositions = new int[this.PrimaryKeyColumns.length];
        for(int i = 0; i < this.PrimaryKeyColumns.length; i++) {
        	for(int j = 0; j < this.CSVHeaders.length; j++) {
        		if(this.CSVHeaders[j].equals(this.PrimaryKeyColumns[i])) {
        			this.pkIndexPositions[i] = j;
        		}
        	}
        }
        
        File File = new File(CSVFileName);
        this.CSVFileName = CSVFileName;
        try {
        	CSVFileReader = new CsvMapReader(new BufferedReader(new FileReader(File)), cp);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
