mkdir /usr/local/customize
curl "https://github.com/plasne/ADFHDIPIG-pipeline/blob/master/XmlStoreFunc/azure-log4j.jar" > /usr/local/customize/azure-log4j.jar
curl "https://github.com/plasne/ADFHDIPIG-pipeline/blob/master/XmlStoreFunc/azure-api-0.4.4.jar" > /usr/local/customize/azure-api-0.4.4.jar
curl "https://pelasnepigstore.blob.core.windows.net/pelasne-pig-2017-03-09t22-26-52-136z/user/plasne/azure-log4j.properties?st=2017-06-05T21%3A51%3A00Z&se=2017-06-30T21%3A51%3A00Z&sp=r&sv=2015-12-11&sr=b&sig=qc8jUd3SYD6pni6xyJC%2FXql8VK%2Fkxv%2FuJgqO5%2B%2BqIGc%3D" > /usr/local/customize/azure-log4j.properties
echo "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/local/customize/*" >> /etc/hadoop/conf/hadoop-env.sh
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/local/customize/*
echo "log4jconf=/usr/local/customize/azure-log4j.properties" >> /etc/pig/conf/pig.properties
