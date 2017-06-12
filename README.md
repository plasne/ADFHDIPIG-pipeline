# Building a data pipeline using Azure Data Factory

## The challenge

A software ISV offers their customers data processing using a proprietary system. The customers might have the raw data in any number of formats, required fields may be spread across multiple files, etc. The goal of this project was to build a flexible pipeline process that could ingest any number of files/records and produce a series of XML files which are in the required format of the proprietary system. Then to collect the files from the system (also in XML) and push those through another pipeline that outputs files that the customer can consume (typically CSV).

In other words...

* customer files -> ingress pipeline -> processor -> egress pipeline -> customer files

There are some requirements of the ingress pipeline:

* The files will be uploaded by the customer to an SFTP site.
* The input files will typically be CSV.
* A large number of records might be processed (up to 10 million).
* A sizable number of different files might be involved (up to 30).
* Some customers need data processed in near-real-time (as low as 30 minute batches).
* There may be multiple pipelines per customer.
* A pipleine process might **require** some files while others might be **optional**.
* The output files will always be XML.

There are some requirements of the egress pipeline:

* The input files will always be XML.
* The output files will typically be CSV.
* The output files will be delivered to an SFTP site.

There was also one very important requirement for the whole solution... the ISV did not want to depend on developers to write the data pipleines. Every customer will have unique requirements, but the ISV wanted to ensure that non-developers could build these unique pipelines using a simple scripting language.

## The solution

A number of technologies were considered but we landed on:

* Azure Data Factory - This orchestrates the flow of data through the pipeline on a set schedule.
* Azure HDInsight - This managed Hadoop environment provided a platform for running the Pig scripts.
* Apache Pig - This data flow language is very easy to use, allowing the non-developers to build the custom scripts for each customer.

There were quite a few customizations that needed to be implemented to meet the requirements - the rest of this article will address those.

## Required and optional files

I mentioned in the requirements that some pipelines would have a collection of required files (for instance, customer and product files might have to be present in order for the processing to work). This is accomplished by XXXXXXXXXXXXXXXXXXXXXXX.

I also mentioned that some sometimes there are optional files (for instance, there could be customer, product, and cost basis files all provided to a pipeline and it could process all of those types, or maybe just one or two if that is all that is available). The solution was:

* The folderPath in [PigInput.json](PigInput.json) should point to a folder containing sub-folders with all the possible files. For example,
  * input-170611T0800
    * customers
      * file1.csv
      * file2.csv
    * products
      * (no files)
    * cost-basis
      * file1.csv
* This entire folder structure will be copied (see example of recursive folder copy in [Normalize.json](Normalize.json)) from an SFTP server to Azure Blob Storage.
* Pig should load all the files in each folder and process them. Unfortunately, all of the default loaders for Pig will throw an exception if it finds the folder missing or an empty folder (both of which are possible).

To solve this loading problem, a custom LoadFunc was written with the following features:

* The folder specified in the Pig LOAD function should be the root folder so that something exists and the process does not immediately throw an exception.
* If the specified sub-folder does not exist or does not contain any files, a set of NULL Tuples is returned from the LoadFunc. This allows all normal processing to happen, but no data will be generated (the files will be 0 bytes).
* Validation rules will determine whether rows are valid or not and what happens when they are not (throw exception, skip record, etc).
* All information about the source data files will be logged (missing files, skipped records, etc).

This LoadFunc is found here as [LoadCsvOrEmpty.java](LoadCsvOrEmpty.java). To compile it, you can run [compile.sh](compile.sh).

## Centralized logging

There are a number of technologies in play that all have their own logging (YARN logs, Pig logs, Java logs, etc.), and while some of those logs can be consolidated by HDInsight, there are others that cannot. This poses a challenge with both coorelation and consolidation.

The solution was to implement a central store for all logs using log4j (which is supported by all the desired platforms). Each platform has a log4j.properties file while contains details about the configuration, including where those logs are stored.

There are a few sample projects on GitHub that implement log4j appenders that can store to Azure Storage. We used XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX.

Of course, the above libraries and all the configuration changes need to be pushed across all servers, so a Script Action was needed. I wrote [deploy.sh](deploy.sh) for this purpose.

THIS SHOULD BE EXTENDED TO INCLUDE A TRANSACTION # (customer-date-time).

A DASHBOARD TO RETURN ALL LOGS FOR A GIVEN TRANSACTION # SHOULD BE BUILT.

Hold processing waiting for files
Skip files

Azure Data Factory, HDInsight, Pig Pipeline Sample

There are 2 parts to this project:
1. XmlStoreFunc - a Java code sample to output XML data from Pig
2. DataFactoryApp1 - a VS project for ADF showing how to build a pipeline to use Pig

## XML StorFunc / RecordWriter

You can use this project with Pig to output XML. The structure will look like this:

```xml
<root_element>
  <entry_element>
    <field_name>field_value</field_name>
    <field_name>field_value</field_name>
  </entry_element>
  <entry_element>
    <field_name>field_value</field_name>
    <field_name>field_value</field_name>
  </entry_element>
</root_element>
```

### Compile

1. javac -classpath "/usr/hdp/2.5.4.0-121/pig/pig-0.16.0.2.5.4.0-121-core-h2.jar:/usr/hdp/2.5.4.0-121/hadoop/hadoop-common.jar:/usr/hdp/2.5.4.0-121/hadoop/client/hadoop-mapreduce-client-core.jar:/usr/hdp/2.5.4.0-121/pig/lib/json-simple-1.1.jar" XML.java
2. md output
3. mv *.class output/
4. jar vcf output.jar output

### Use

1. REGISTER output.jar
2. Make sure your fields are defined as field_name:data_type (see output.pig for an example)
3. STORE <tuples> INTO '/path/output.xml' USING output.XML('root_element', 'entry_element');

### Execute

1. pig output.pig

## ADF Pipeline

This sample was written using Visual Studio 2015 and the Data Factory Tools for VS2015. It does the following:
1. pulls files from an FTP server
2. deposits them in an Azure Storage Blob container
3. runs a Pig script to normalize the data and put it into XML
4. deposits the output files in an Azure Storage Blob container

### NOTES

* fully qualified names for jar files in pig DEFINE
* pig -x local local.pig
