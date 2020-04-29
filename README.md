# MarkLogic PreJoiner
Loading relational data into MarkLogic using the Document-per-row approach is an anti-pattern due to the high amount of both CPU and disk overhead.  Joining these documents before loading can dramatically increase both ingestion and harmonization performance.  The marklogic-pre-joiner tool is for joining many rows from individual CSV files into larger XML documents and then loading them into a MarkLogic database.  It was designed to be fast, memory efficient, and scalable to thousands of CSV files taking petabytes of disk space.

This process occurs in three steps:
1. A primary key column, or collection of columns used to compose a primary key, is identified and all CSV files are sorted on this key.  I recommend BurntSushi's cli CSV tool, [XSV](https://github.com/BurntSushi/xsv "A fast CSV command line toolkit written in Rust"), to do this.
2. The Java tool will scan the primary CSV file identified in the properties file to build an entity, extract the primary key, use that key to find any child rows in associated CSV files, then build a full XML document containing all the information.
3. The XML document is then sent to [a bulk Data Service loader](https://docs.marklogic.com/guide/java/DataServices "Java Application Developer's Guide - Data Services") for batching and distributed loading into the MarkLogic cluster.

## Configuring the Tool
The tool is driven by the properties.xml file found in the root directory.  It's split into two sections:
1. Connection : The connection element contains all information used for connecting to your MarkLogic cluster
   * hosts : 
   * port : 
   * numThreadsPerHost : 
   * batchSize : 
   * username : 
   * password : 
2. Data
   * entity :
   * namespace :
   * primaryCSV :
   * childCSVs : 
   All CSV nodes have the following elements
      * location : 
      * header : 
      * primaryKey : 
      * name : 
      * separator : 

## Using the Tool

## Dependencies
