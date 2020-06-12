package com.marklogic.dataservices.prejoiner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.jdom2.Element;
import org.supercsv.prefs.CsvPreference;
import java.lang.StringBuilder;

public class CSVRowConsumer {
	private final BlockingQueue<Element> csvQueue = new LinkedBlockingQueue<>(3);
	private final CSVRowProducer csvRowProducer;

	private Element currentElement;
	
	String exitFlag = "Done";
	String[] csvPrimaryKeyColumnNames;
	String csvEntityName;
	String csvLocation;
	
	public CSVRowConsumer(CsvPreference cp, String csvLocation, String csvHeader, String csvPrimaryKeyColumnNames, String csvEntityName) throws InterruptedException{
		this.csvLocation = csvLocation;
		this.csvPrimaryKeyColumnNames = csvPrimaryKeyColumnNames.split(",");
		this.csvEntityName = csvEntityName;
		csvRowProducer = new CSVRowProducer(cp, csvQueue, csvLocation, csvHeader, this.csvPrimaryKeyColumnNames, csvEntityName);
		new Thread(csvRowProducer).start();
		currentElement = csvQueue.take();
	}
	
	public Element getNextElement() throws InterruptedException {
		if(exitFlag.equals(currentElement.getName())) {
			return currentElement;
		} else {
			Element returnElement = currentElement;
			currentElement = csvQueue.take();
			return returnElement;
		}
	}
	
	public Element findChildOffQueue(String primaryKeyTarget) throws InterruptedException{
		boolean keepScanning = true;
		StringBuilder sb = new StringBuilder("");
		do {
			if(!exitFlag.equals(currentElement.getName())) {
				for(int i = 0; i < csvPrimaryKeyColumnNames.length; i++) {
					sb.append(currentElement.getChild(csvEntityName).getChildText(csvPrimaryKeyColumnNames[i]).trim());
				}
				String childPrimaryKeyValue = sb.toString();
				
				if(childPrimaryKeyValue.compareTo(primaryKeyTarget) == 0) {
					return currentElement;
				} else if (childPrimaryKeyValue.compareTo(primaryKeyTarget) < 0) {
					currentElement = csvQueue.take();
					sb.setLength(0);
				} else {
					return null;
				}
			} else {
				return currentElement;
			}
		} while (keepScanning);
		return null;
	}
}
