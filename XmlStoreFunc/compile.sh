javac -classpath "/usr/hdp/2.5.4.0-121/pig/pig-0.16.0.2.5.4.0-121-core-h2.jar:/usr/hdp/2.5.4.0-121/hadoop/hadoop-common.jar:/usr/hdp/2.5.4.0-121/hadoop/client/hadoop-mapreduce-client-core.jar:/usr/hdp/2.5.4.0-121/pig/lib/json-simple-1.1.jar" XML.java
md output
mv *.class output/
jar vcf output.jar output