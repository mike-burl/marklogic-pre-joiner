# MarkLogic PreJoiner
Loading relational data into MarkLogic using the Document-per-row approach is an anti-pattern due to the high amount of both CPU and disk overhead.  Joining these documents before loading can dramatically increase both ingestion and harmonization performance.  The marklogic-pre-joiner tool is for joining many rows from individual CSV files into larger XML documents and then loading them into a MarkLogic database.  It was designed to be fast, memory efficient, and scalable to thousands of CSV files taking petabytes of disk space.

This process occurs in three steps:
1. A primary key column, or collection of columns used to compose a primary key, is identified and all CSV files are sorted on this key into lexicographic order.  I recommend BurntSushi's cli CSV tool, [XSV](https://github.com/BurntSushi/xsv "A fast CSV command line toolkit written in Rust"), to do this.
2. The Java tool will scan the primary CSV file identified in the properties file to build an entity, extract the primary key, use that key to find any child rows in associated CSV files, then build a full XML document containing all the information.
3. The XML document is then sent to [a bulk Data Service loader](https://docs.marklogic.com/guide/java/DataServices "Java Application Developer's Guide - Data Services") for batching and distributed loading into the MarkLogic cluster.

## Configuring the Tool
The tool is driven by the **properties.xml** file found in the root directory.  It's split into two sections:
1. **Connection** : The connection element contains all information used for connecting to your MarkLogic cluster
   * **hosts** : List of addresses for all the E-nodes in the cluster
   * **port** : The port number associated with the MarkLogic database's application server
   * **numThreadsPerHost** : The number of concurrent threads that will load batches of documents to each node in the cluster
   * **batchSize** : The maxmimum number of documents that will be sent in each batch
   * **username** : The MarkLogic username that will be used to load the documents
   * **password** : The password to the above username
2. **Data** : The data element defines the CSV files to be ingested along with the structure the final document will take
   * **entity** : The name of the root entity represented by the full XML document
   * **namespace** : The XML document's root namespace
   * **primaryCSV** : This CSV is the root file that will be used to drive the document generation process.  All children will be joined using its primary key.
   * **childCSVs** : All other CSV files being joined
   All CSV nodes have the following elements
      * **location** : The file location of the CSV file
      * **header** : The comma-separated header
      * **primaryKey** : The primary key column or comma-separated collection of columns used to build the primary key
      * **name** : The root name of the CSV
      * **separator** : The separating character used in the CSV file.  By default this is a , character

Gradle is used for deploying the MarkLogic-side of the data service and can also be used to build and run the tool.  The settings for this are located in the **gradle.properties** file located in the root directory.

## Using the Tool
1. **Deploy the Data Service** : Use gradle to deploy the data service to the MarkLogic cluster with the command './gradlew mlLoadModules -i'.  Verify that a module with the name bulkLoader.xqy was inserted into the modules database.
2. **Build the Prejoiner App** : Use gradle to build the prejoiner Java app with the command './gradlew build'.  This will automatically resolve all dependencies the tool needs to run.
3. **Run the Prejoiner** : Use gradle to launch the prejoiner Java app with the command './gradlew runJava'.  Alternatively, you can launch the jar file that was built in the previous step directly for jobs that will take a very long time and need to be run in a headless state.
4. **That's it!** : Monitor both the MarkLogic cluster and ingestion server to ensure resources are being utilized effectively.  Modify the thread and batch parameters as you see fit.

## Dependencies
* [MarkLogic Server](https://developer.marklogic.com/products/marklogic-server/10.0 "MarkLogic 10 - MarkLogic Developer Community")
* [Gradle](https://gradle.org/ "Gradle Build Tool")
* [Java Virtual Machine 1.8](https://www.java.com/en/ "Java | Oracle")