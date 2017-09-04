# SftpReset

## Problem

Some customers couldn't put files on the SFTP server in the format required (contained in folders with a UTC timeslice in the folder name).

## Solution

This project is a MapReduce activity that can be scheduled as the first activity in an Azure Data Factory pipeline to login to an SFTP site, rename an folder so it has a UTC timeslice, and then create an empty folder with the original name. Using this, the customer can simply place their files directly into the same folder each time and the ADF will create the timeslice before the other activities run.

## Compiling

You can use the following steps to compile:

```bash
javac -classpath "/usr/hdp/2.5.5.3-2/hadoop/hadoop-common-2.7.3.2.5.5.3-2.jar:/usr/hdp/2.5.5.3-2/hadoop-mapreduce/hadoop-mapreduce-client-core-2.7.3.2.5.5.3-2.jar:SftpReset.jar" SftpReset.java
mv *.class com/plasne/
jar vcf SftpReset.jar com
```

You will notice that the SftpReset.jar is used in its own compilation. The MapReduce jar needs to have all dependencies self-contained in the JAR. Fortunately the only dependency is Jsch, which I have contained in the "com" folder so it is compiled in the SftpReset.jar each time. You could reference the jsch.jar instead or you could just compile using Jsch in the SftpReset.jar each time.

## Configuration

There is a configuration file (shown below as sftp.txt) that must contain rows like this:

```text
input=inputfolder
output='inputfolder-'YYYYMMdd'T'HHmm
hostname=ftp.server.com
username=myusername
password=mypassword
```

It can optionally also contain rows like this:

```text
offset=0
roundTo=60
```

The data will be used in this way:

* input - This is the path to the folder that will be renamed on the SFTP server. It is the same folder name that will be recreated when the rename is done.

* output - This is the path to what the folder will be renamed to. It must be in a format that the Java DataTimeFormatter can parse. For example, you must use single quotes around any literals.

* hostname - This is the fully qualified host name of the SFTP server.

* username/password  - This is a username and password that can be used to login to the SFTP server and that will have the rights to rename and create folders.

## Running Local

To test the job, you can run it locally on an HDInsight server by typing the following:

```bash
hadoop jar SftpReset.jar com.plasne.SftpReset --offset 0 --roundTo 60 --input "/user/plasne/sftp.txt" --output "'/user/plasne/output-'yyyyMMdd'T'HHmm" --local
```

The parameters are:

* offset - This is the number of minutes to add to the current time. You might use this to make the timeslice represent the future or the past (negative number).

* roundTo - This is the maximum number of minutes to round up (positive number) or round down (negative number). Some examples:
  * If set to 15, it will round up to the next 15 minute increment, ex. 11:43am would become 11:45am.
  * If set to -30, it will round down to the next 30 minute increment, ex. 11:43am would become 11:30am.
  * If set to 60, it will round to the next hour, ex. 11:43am would become noon.
  
* input - This is the file containing the other parameters, most notably the credentials to get to the SFTP site.

* output - This is the folder that will be created for the files output from the MapReduce activity. There won't actually be any data, but you must have this folder specified such that it can be created or the job will fail. Because this folder will need to be created on every run you must have a timeslice in the name so the output must be in a format that can be parsed by the Java DataTimeFormatter. For example, you must use single quotes around any literals.

* local - For some reason I could never determine, the Map and Reduce classes cannot be found by setJarByClass when running local and cannot be found by setJar when running remote (actually this second one is because the file is renamed on import), so flagging it local or not changes the logic on how the JAR is referenced.

* realtime - The default behavior is to create the output directory for the MapReduce job use the same timeslice as is created for the SFTP folder (ie. if the output is normalized to 20170810T2100 then that same 2100 hours is used for the output of the MapReduce job). This is fine if you aren't using a retry policy, but if you decide you need one, this won't work because the output folder will likely get created immediately, the job will fail, and then no further jobs can run because the folder is already created. To counteract that, you can use the realtime flag. If you do, you don't have to the specify the --offset or --roundTo parameters (you would need to specify them in the input file or the command line, either is fine). When you use realtime, the output folder for the MapReduce job uses the current UTC timestamp (of course the SFTP will still use the offset and roundTo). This means if you have a retry of 5 minutes, you could have the timeslice for the output folder be every minute and avoid conflicts.

## Running on ADF

To run the MapReduce activity on ADF, you just need to add an activity to your pipeline like the following:

```json
{
    "type": "HDInsightMapReduce",
    "typeProperties": {
        "className": "com.plasne.SftpReset",
        "jarFilePath": "pelasne-pig-2017-03-09t22-26-52-136z/user/plasne/SftpReset.jar",
        "jarLinkedService": "UnencryptedStorage",
        "arguments": [
            "--offset",
            "0",
            "--roundTo",
            "60",
            "--input",
            "wasb://pelasne-pig-2017-03-09t22-26-52-136z@pelasnepigstore.blob.core.windows.net/user/plasne/sftp.txt",
            "--output",
            "'wasb://pelasne-pig-2017-03-09t22-26-52-136z@pelasnepigstore.blob.core.windows.net/user/plasne/bogusoutput-'yyyyMMdd'T'HHmm"
        ]
    },
    "outputs": [
        {
            "name": "BogusOutput"
        }
    ],
    "policy": {
        "timeout": "01:00:00",
        "concurrency": 1,
        "retry": 1
    },
    "scheduler": {
        "frequency": "Hour",
        "interval": 1
    },
    "name": "SftpResetActivity",
    "description": "Custom MapReduce to rename a folder in SFTP and create a new one.",
    "linkedServiceName": "HDInsight"
},

```

You don't need to specify any inputs for ADF, but of course the --input parameter must be valid. The output is required because it is going to be tracked to start the subsequent copy activities. The BogusOutput might look like this:

```json
{
    "name": "BogusOutput",
    "properties": {
        "published": false,
        "type": "AzureBlob",
        "linkedServiceName": "UnencryptedStorage",
        "typeProperties": {
            "folderPath": "input/user/plasne/bogusoutput-{date}T{time}",
            "partitionedBy": [
                {
                    "name": "date",
                    "value": {
                        "type": "DateTime",
                        "date": "SliceEnd",
                        "format": "yyyyMMdd"
                    }
                },
                {
                    "name": "time",
                    "value": {
                        "type": "DateTime",
                        "date": "SliceEnd",
                        "format": "HHmm"
                    }
                }
            ]
        },
        "availability": {
            "frequency": "Hour",
            "interval": 1
        }
    }
}
```

It doesn't matter for this activity whether the folder path is valid or even matches what you specified in the parameter, it won't be used, it just needs to be defined.

The COPY activity or activities that follow should look like this:

```json
{
    "type": "Copy",
    "typeProperties": {
        "source": {
            "type": "FileSystemSource",
            "recursive": true,
            "sourceRetryCount": 1
        },
        "sink": {
            "type": "BlobSink",
            "blobWriterOverwriteFiles": true,
            "writeBatchSize": 0,
            "writeBatchTimeout": "00:00:00"
        }
    },
    "inputs": [
        {
            "name": "BogusOutput"
        },
        {
            "name": "FtpInput-CUSTOMER"
        }
    ],
    "outputs": [
        {
            "name": "FtpOutput-CUSTOMER"
        }
    ],
    "policy": {
        "timeout": "01:00:00",
        "concurrency": 4,
        "retry": 1
    },
    "scheduler": {
        "frequency": "Hour",
        "interval": 1
    },
    "name": "CopyCustomerFiles"
},
```

Note that for inputs, I have both the BogusOutput and the correct location for the input files on the SFTP server - this forces the activity to wait until BogusOutput is done (meaning the MapReduce job has completed successfully). This is of course a requirement because you cannot pick up the files unless they are in the newly created timeslice folder.

Finally the input activity might look something like this:

```json
{
    "name": "FtpInput-CUSTOMER",
    "properties": {
        "published": false,
        "type": "FileShare",
        "linkedServiceName": "FTP",
        "typeProperties": {
            "fileFilter": "CUSTOMER*",
            "useBinaryTransfer": "True",
            "folderPath": "hourly/hourly-{date}T{time}",
            "partitionedBy": [
                {
                    "name": "date",
                    "value": {
                        "type": "DateTime",
                        "date": "SliceEnd",
                        "format": "yyyyMMdd"
                    }
                },
                {
                    "name": "time",
                    "value": {
                        "type": "DateTime",
                        "date": "SliceEnd",
                        "format": "HHmm"
                    }
                }
            ]
        },
        "availability": {
            "frequency": "Hour",
            "interval": 1
        },
        "external": true,
        "policy": {}
    }
}
```

Notice it is looking for the folder path with the timeslice that cooresponds to the newly created folder.
