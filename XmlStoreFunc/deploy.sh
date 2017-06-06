mkdir /usr/local/customize
curl -o /usr/local/customize/azure-log4j.jar "https://raw.githubusercontent.com/plasne/ADFHDIPIG-pipeline/master/XmlStoreFunc/azure-log4j.jar"
curl -o /usr/local/customize/azure-api-0.4.4.jar "https://raw.githubusercontent.com/plasne/ADFHDIPIG-pipeline/master/XmlStoreFunc/azure-api-0.4.4.jar"
curl -o /usr/local/customize/azure-log4j.properties "https://pelasnepigstore.blob.core.windows.net/pelasne-pig-2017-03-09t22-26-52-136z/user/plasne/azure-log4j.properties?st=2017-06-05T21%3A51%3A00Z&se=2017-06-30T21%3A51%3A00Z&sp=r&sv=2015-12-11&sr=b&sig=qc8jUd3SYD6pni6xyJC%2FXql8VK%2Fkxv%2FuJgqO5%2B%2BqIGc%3D"
printf "\n\nexport HADOOP_CLASSPATH=\44HADOOP_CLASSPATH:/usr/local/customize/*" >> /etc/hadoop/conf/hadoop-env.sh
hadoop classpath
printf "\n\nlog4jconf=/usr/local/customize/azure-log4j.properties" >> /etc/pig/conf/pig.properties
