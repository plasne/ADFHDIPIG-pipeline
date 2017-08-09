javac -classpath "/usr/hdp/2.5.5.3-2/hadoop/hadoop-common-2.7.3.2.5.5.3-2.jar:/usr/hdp/2.5.5.3-2/hadoop-mapreduce/hadoop-mapreduce-client-core-2.7.3.2.5.5.3-2.jar:SftpReset.jar" SftpReset.java
mv *.class com/plasne/
jar vcfm SftpReset.jar Manifest.txt com
