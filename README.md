# Azure Data Factory, HDInsight, Pig Pipeline Sample

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

1. javac -classpath "/usr/hdp/2.5.4.0-121/pig/pig-0.16.0.2.5.4.0-121-core-h2.jar:/usr/hdp/2.5.4.0-121/hadoop/hadoop-common.jar:/usr/hdp/2.5.4.0-121/hadoop/client/hadoop-mapreduce-client-core.jar" XML.java
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
