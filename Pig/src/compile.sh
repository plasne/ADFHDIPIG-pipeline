javac -classpath "/usr/hdp/2.6.0.10-29/pig/pig-0.16.0.2.6.0.10-29-core-h2.jar:/usr/hdp/2.6.0.10-29/hadoop/hadoop-common.jar:/usr/hdp/2.6.0.10-29/hadoop/client/hadoop-mapreduce-client-core.jar:/usr/hdp/2.6.0.10-29/pig/lib/json-simple-1.1.jar" XML.java
mkdir output
mv *.class output/
jar vcf ../lib/output.jar output
javac -classpath "/usr/hdp/2.6.0.10-29/pig/pig-0.16.0.2.6.0.10-29-core-h2.jar:/usr/hdp/2.6.0.10-29/hadoop/hadoop-common.jar:/usr/hdp/2.6.0.10-29/hadoop/client/hadoop-mapreduce-client-core.jar:/usr/hdp/2.6.0.10-29/pig/lib/json-simple-1.1.jar:/usr/hdp/2.6.0.10-29/pig/piggybank.jar:/usr/local/customize/azure-api-0.4.4.jar" LogEntity.java LoadCsvOrEmpty.java
mkdir input
mv *.class input/
jar vcf ../lib/input.jar input