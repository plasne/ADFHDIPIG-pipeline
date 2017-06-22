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
* Sometimes the output files should POST to a webservice.

There was also one very important requirement for the whole solution... the ISV did not want to depend on developers to write the data pipleines. Every customer will have unique requirements, but the ISV wanted to ensure that non-developers could build these unique pipelines using a simple scripting language.

## The solution

A number of technologies were considered but we landed on:

* Azure Data Factory - This orchestrates the flow of data through the pipeline on a set schedule.
* Azure HDInsight - This managed Hadoop environment provided a platform for running the Pig scripts.
* Apache Pig - This data flow language is very easy to use, allowing the non-developers to build the custom scripts for each customer.

There were quite a few customizations that needed to be implemented to meet the requirements - the rest of this article will address those.

## Manifest

There are quite a few folders and files in this repository, so before I get into the specifics, I will provide an overview of what is contained here:

* DataFactoryApp1 - The Visual Studio sample for an ADF pipeline.
* Monitor - A Node.js app for viewing the logs (see [Troubleshooting](#troubleshooting) below).
* Pig
  * lib - JAR files that are needed to compile and run these samples.
  * logging - The files for configuring centralized logging for log4j.
    * azure-log4j.properties - A configuration file for log4j to log to Azure Blob Storage.
    * deploy.sh - A sample Script Action for HDInsight to deploy the logging changes.
  * src - The Java source code for the LoadFunc and StoreFunc.
  * sample - Sample data files.
  * full.pig - A sample Pig script showing the LoadFunc and StoreFunc.
  * optional.pig - A sample Pig script showing how you could handle missing folders or files without using a LoadFunc. Ultimately this was not used because of additional functionality needed in the LoadFunc (such as logging).
  * local.pig - A sample Pig script showing local files instead of using HDFS or WASB.
  * validate.json - A sample configuration file for the LoadFunc.
  * config.json - A sample configuration file for the StoreFunc showing all options.
  * minimal.json - A sample configuration file for the StoreFunc showing minimal options.

## ADF pipeline

The [DataFactoryApp1](DataFactoryApp1) sample was written using Visual Studio 2015 and the Data Factory Tools for VS2015. It does the following:

1. Pulls files from an FTP server.
2. Deposits them in an Azure Storage Blob container.
3. Runs a Pig script to normalize the data and put it into XML.
4. Deposits the output files in an Azure Storage Blob container.

This is not comprehensive, but rather shows a representative sample of the activities that will be built into the pipelines.

## Required and optional files

I mentioned in the requirements that some pipelines would have a collection of required files (for instance, customer and product files might have to be present in order for the processing to work). This is accomplished by specifying multiple *input* folders in the activity - Data Factory will wait for all the inputs to be available before running the activity. For example:

```json
"inputs": [
  {
    "name": "test-ppiv4-001-customer-dev-ds-piginput"
  },
  {
    "name": "test-ppiv4-001-customer-dev-ds-piginputB"
  }
],
```

I also mentioned that some sometimes there are optional files (for instance, there could be customer, product, and cost basis files all provided to a pipeline and it could process all of those types, or maybe just one or two if that is all that is available). The solution was:

* The folderPath in [PigInput.json](DataFactoryApp1/DataFactoryApp1/PigInput.json) should point to a folder containing subfolders with all the possible files. For example,
  * input-170611T0800
    * customers
      * file1.csv
      * file2.csv
    * products
      * (no files)
    * cost-basis
      * file1.csv
* This entire folder structure will be copied (see example of recursive folder copy in [Normalize.json](DataFactoryApp1/DataFactoryApp1/Normalize.json)) from an SFTP server to Azure Blob Storage.
* Pig should load all the files in each folder and process them. Unfortunately, all of the default loaders for Pig will throw an exception if it finds the folder missing or an empty folder (both of which are possible).

To solve this loading problem, a custom LoadFunc was written with the following features:

* The folder specified in the Pig LOAD function should be the root folder so that something exists and the process does not immediately throw an exception.
* If the specified subfolder does not exist or does not contain any files, a set of NULL Tuples is returned from the LoadFunc. This allows all normal processing to happen, but no data will be generated (the files will be 0 bytes).
* Validation rules will determine whether rows are valid or not and what happens when they are not (throw exception, skip record, etc).
* All information about the source data files will be logged (missing files, skipped records, etc).

This LoadFunc is found here as [LoadCsvOrEmpty.java](Pig/src/LoadCsvOrEmpty.java). To compile it, you can run [compile.sh](Pig/src/compile.sh).

To use it in a Pig script...

```pig
REGISTER lib/input.jar;
REGISTER lib/json-simple-1.1.jar;
REGISTER /usr/local/customize/azure-api-0.4.4.jar;
REGISTER /usr/hdp/2.5.4.0-121/pig/piggybank.jar;

raw = LOAD '/user/plasne' USING input.LoadCsvOrEmpty('customer-20170620T1100', 'input', 'empty', '/user/plasne/validate.json');
```

All 4 of those JAR files must be registered. You specify to load the root folder (user/plasne) which must exist, the the following parameters:

* instanceId - This will be used as the partitionKey in the Azure Table for logging (see [Troubleshooting](#troubleshooting) below). It is the way to identify this particular job. You will need to generate this programmatically, for ex. Acme-20170620T1100 might be a good instanceId.
* proposed subfolder - This will be the subfolder that contains the files you are trying to load (which may or may not exist or contain files).
* empty subfolder - This will be the subfolder that contains nothing (or an empty file in the case of WASB). This is used when the subfolder doesn't exist or doesn't have files.
* configuration - This is the location of a JSON configuration file that contains the schema for the files and how to handle validation failures.

A sample configuration file has been provided [validate.json](Pig/validate.json).

Note that it is not necessary to provide the schema to the Pig LOAD function, that will be projected from the configuration file. "onWrongType" can be "skip" (the record is skipped, but this is logged) or "fail" (an exception is thrown and Pig aborts).

## Saving to XML

Piggybank provides an XML loader, but not a store function, so one had to be written. The StoreFunc can be found here as [XML.java](Pig/src/XML.java). To compile it, you can run [compile.sh](Pig/src/compile.sh).

This is a very simple conversation that will make each row into a node and each column into a node beneath the row node. For example, this...

```
Fabrikam,20170611T0900,21037
ACME Corp,20170611T0800,17635
```

...can become this...

```xml
<root>
  <entry>
    <customer_name>Fabrikam</customer_name>
    <submission_date>20170611T0900</submission_date>
    <amount>21037</amount>
  </entry>
  <entry>
    <customer_name>ACME Corp</customer_name>
    <submission_date>20170611T0800</submission_date>
    <amount>17635</amount>
  </entry>
</root>
```

The schema Pig sees for the data will determine the names of the columns (nodes under the entry), therefore it is important to specify the schema on load using AS or to use the [LoadCsvOrEmpty.java](Pig/src/LoadCsvOrEmpty.java) LoadFunc which projects a schema from the configuration file.

There were a number of features implemented in this StoreFunc:

* The rows are saved as simple XML.
* Special processors can be defined that generate something other than simply a node per column.
  * The "scale" processor takes input like this: **(qty,value;qty,value;qty,value)** and produces something like this: **\<SCALE\>\<QTY\>#\</QTY\><VAL\>#\</VAL\>\<QTY\>#\</QTY\><VAL\>#\</VAL\>\<QTY\>#\</QTY\><VAL\>#\</VAL\>\<SCALE\>**.
  * The processor supports any number of columns (separated by commas), any number of rows (separated by semi-colons), and user-defined names for those columns (defined in the configuration file).
* Additional lines of content can be added as a header (pre) or footer (post) to the XML data. This is useful to meet 2 requirements:
  * When the XML file is POST to a webservice, it typically needs to be SOAP, this allows you to create the envelope.
  * If the XML root node needs a namespace, you can leave the "root" parameter empty and instead put an appropriate pre/post.
* Shell actions can be started when the output file is closed. For example, curl can be used to POST the file to a webservice. If the action does not complete successfully, an exception is thrown and the Pig script fails.

To use it in a Pig script...

```pig
REGISTER lib/output.jar;
REGISTER lib/json-simple-1.1.jar;

STORE data INTO '/user/plasne/output.xml' USING output.XML
('/user/plasne/config.json');

--OR

STORE data INTO '/user/plasne/output.xml' USING output.XML('root', 'entry');
```

Both of those JAR files must be registered. For most cases, the StoreFunc takes a single parameter which is the location of a JSON configuration file. However, if you no requirements beyond specifying the root and entry node names, you can simply specify them both as parameters.

A sample configuration file has been provided [config.json](Pig/config.json). The options in the configuration file include:

* root [optional] - The name of the root node for the XML file. If it not specified, no root node or associated closing tag will be created (this is typically because you are going to specify that information in pre/post).
* entry [required] - The name of the node that contains each row.
* processors [optional] - An array of processors that can output XML in a more customized way (see below).
* pre [optional] - An array of strings that will be added to the beginning of the XML file as a header.
* post [optional] - An array of strings that will be added to the end of the XML file as a footer.
* onclose [optional] - An array of strings that will be run in the Linux shell once the file is closed (for example, using curl to POST to a webservice).

There is only one processor currently (scale). It is described above, but the configuration options are:

* column [required] - The name of the source column containing the compacted data.
* type [required] - The name of the processor. This must be "scale".
* node [optional] - The name of the node that is created to hold the children. If it is not provided, it will use the column name.
* children [required] - The name of the child nodes that will be created.

## Centralized logging

There are a number of technologies in play that all have their own logging (YARN logs, Pig logs, Java logs, etc.), and while some of those logs can be consolidated by HDInsight, there are others that cannot. This poses a challenge with both coorelation and consolidation.

The solution was to implement a central store for all logs using log4j (which is supported by all the desired platforms). Each platform has a log4j.properties file while contains details about the configuration, including where those logs are stored.

There are a few sample projects on GitHub that implement log4j appenders that can store to Azure Storage. I used https://github.com/JMayrbaeurl/azure-log4j. It also needs the 0.4.4 version of the Azure SDK for Java, the JAR for which can be found here: https://mvnrepository.com/artifact/com.microsoft.windowsazure/microsoft-windowsazure-api/0.4.4.

Of course, the above libraries and all the configuration changes need to be pushed across all servers, so a Script Action was needed. I wrote [deploy.sh](Pig/logging/deploy.sh) for this purpose.

## <a name="troubleshooting"></a> Troubleshooting

As previously mentioned, the LoadCsvOrEmpty LoadFunc can be used to handle optional files and validation. When that LoadFunc is used it logs what it discovers and how it handles it to an Azure Table. You can then use the included Node.js monitoring app in [/Monitor](Monitor) to view those logs.

![log screenshot](/images/log-ss.png)

You must load a log by instanceId, so it is a good idea to have a naming scheme that is documented somewhere and used consistently. There is no specific way to associate the YARN, Pig, etc. logs with the instance logs, but when you click on an entry in the Instance Logs the Associated Logs will load showing everything 5 minutes before through 5 minutes after.

To run the application, you must have Node.js installed. You can then execute with the following command:

```bash
node server.js
```

The application presents a web service running on port 80, so you may need to use "sudo" in order to bind to that port. You can then access the application by typing "http://localhost" into a browser.

## Other notes

When storing JAR files in WASB (the HDFS-compliant file store protocol for HDInsight), you must use fully qualified paths to the files (wasbs://storageaccounturl/containername/path/file). This is required for authentication.
