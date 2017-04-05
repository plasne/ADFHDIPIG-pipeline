# XML StorFunc / RecordWriter

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

## Compile

1. javac -classpath "/usr/hdp/2.5.4.0-121/pig/pig-0.16.0.2.5.4.0-121-core-h2.jar:/usr/hdp/2.5.4.0-121/hadoop/hadoop-common.jar:/usr/hdp/2.5.4.0-121/hadoop/client/hadoop-mapreduce-client-core.jar" XML.java
2. md output
3. mv *.class output/
4. jar vcf output.jar output

## Use

1. REGISTER output.jar
2. Make sure your fields are defined as field_name:data_type (see output.pig for an example)
3. STORE <tuples> INTO '/path/output.xml' USING output.XML('root_element', 'entry_element');

## Execute

1. pig output.pig
