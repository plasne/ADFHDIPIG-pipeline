
## Query Azure Table using Azure SDK 0.4.4

I used a different way to generate the log indexes, but just in case it is necessary to use Java and the old SDK to read data from a table, here is an example.

```java
// make the instance index one higher than the last
TableQuery<LogEntity> query = TableQuery.from(logging_tableName, LogEntity.class).where("(PartitionKey eq '" + instanceId + "')");
  for (LogEntity entity : cloudTable.getServiceClient().execute(query)) {
    String[] id = entity.getRowKey().split("-");
    int consider = Integer.parseInt(id[0]);
    if (consider > instanceIndex) instanceIndex = consider;
  }
instanceIndex++;
```
