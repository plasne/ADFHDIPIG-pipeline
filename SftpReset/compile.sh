javac -classpath "/usr/hdp/2.5.5.3-2/hadoop/hadoop-common-2.7.3.2.5.5.3-2.jar:/usr/hdp/2.5.5.3-2/hadoop-mapreduce/hadoop-mapreduce-client-core-2.7.3.2.5.5.3-2.jar:/usr/hdp/2.5.5.3-2/pig/lib/json-simple-1.1.jar:jsch.jar" SftpReset.java
mv SftpReset.class com/plasne/SftpReset.class
jar vcfm SftpReset.jar Manifest.txt com

