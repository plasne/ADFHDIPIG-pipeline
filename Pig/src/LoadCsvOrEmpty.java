package input;

// support local filesystem
// support validation
// write out logs
// support configuration of an empty directory

import java.lang.Integer;
import java.util.Scanner;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.Expression;
import org.apache.pig.piggybank.storage.CSVLoader;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.microsoft.windowsazure.serviceruntime.RoleEnvironment;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageCredentialsAccountAndKey;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTable;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableServiceEntity;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import com.microsoft.windowsazure.services.table.client.TableQuery.QueryComparisons;

class Column {
  public String name;
  public String type;
  public String onWrongType;

  public Column(String name, String type, String onWrongType) {
    this.name = name;
    this.type = type;
    this.onWrongType = onWrongType;
  }

}

public class LoadCsvOrEmpty extends CSVLoader implements LoadMetadata {

  private boolean hasFiles = false;
  private String instanceId;
  private int instanceIndex;
  private int logEntryIndex;
  private String target;
  private String config;
  private ArrayList<Column> columns = new ArrayList<Column>();
  private String logging_storageAccount;
  private String logging_accountKey;
  private String logging_tableName;
  private CloudTable cloudTable;
  private String filename;
  private int rowIndex;

  public LoadCsvOrEmpty(String instanceId, String target, String config) {
    this.instanceId = instanceId;
    this.target = target;
    this.config = config;
  }

  @Override
  public Tuple getNext() throws IOException {
    if (hasFiles) {
      Tuple t;
      boolean skipped = false;
      do {
        rowIndex++;
        t = super.getNext();
        skipped = false;
        if (t != null) {

          // verify number of columns
          int size = columns.size();
          if (t.size() != size) {
            String size_mismatch = "[" + filename + ", line:" + rowIndex + "]: expected " + size + " columns, but found " + t.size() + ".";
            log("FAIL", size_mismatch);
            throw new ExecException(size_mismatch, 2200, PigException.BUG);
          }

          for (int i = 0; i < size; i++) {
            if (!skipped) {
              byte type = t.getType(i);
              Object value = t.get(i);
              Column column = columns.get(i);
              switch (column.type.toLowerCase()) {
                case "bool":
                case "boolean":
                  try {
                    t.set(i, DataType.toBoolean(value));
                  } catch (Exception ex) {
                    String typecast_fail = "[" + filename + ", line:" + rowIndex + ", column:" + i + "]: a boolean was expected " + size + " columns, but the value was '" + value + "'.";
                    if (column.onWrongType.equals("skip")) {
                      log("SKIP", typecast_fail);
                      skipped = true;
                    } else {
                      throw new ExecException(typecast_fail, 2201, PigException.BUG);
                    }
                  }
                  break;
                case "int":
                case "integer":
                  try {
                    t.set(i, DataType.toInteger(value));
                  } catch (Exception ex) {
                    String typecast_fail = "[" + filename + ", line:" + rowIndex + ", column:" + i + "]: an integer was expected " + size + " columns, but the value was '" + value + "'.";
                    if (column.onWrongType.equals("skip")) {
                      log("SKIP", typecast_fail);
                      skipped = true;
                    } else {
                      throw new ExecException(typecast_fail, 2201, PigException.BUG);
                    }
                  }
                  break;
                case "number":
                case "double":
                  try {
                    t.set(i, DataType.toDouble(value));
                  } catch (Exception ex) {
                    String typecast_fail = "[" + filename + ", line:" + rowIndex + ", column:" + i + "]: a double was expected " + size + " columns, but the value was '" + value + "'.";
                    if (column.onWrongType.equals("skip")) {
                      log("SKIP", typecast_fail);
                      skipped = true;
                    } else {
                      throw new ExecException(typecast_fail, 2201, PigException.BUG);
                    }
                  }
                  break;
              }
            }
          }

        }
      } while (skipped);
      if (t == null) {
        log("INFO", "Completed reading from file: " + filename + ".");
      }
      return t;

    } else {
      return null;
    }
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    super.prepareToRead(reader, split);
    String new_filename = ((FileSplit)split.getWrappedSplit()).getPath().getName();
    if (new_filename.equals(filename)) {
      rowIndex = 0;
      log("INFO", "Started reading from file: " + filename + ".");
    }
  }

  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    return null;
  }

  public String[] getPartitionKeys(String location, Job job) throws IOException {
    return null;
  }

  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    // nothing to do
  }

  private void readConfig(String location, Job job) throws IOException {
    if (columns.size() < 1) {

      // read the configuration
      try {
        String raw;
        
        // support local and hadoop
        if (config.startsWith("./")) {

          // read from the local file system
          raw = new String(Files.readAllBytes(Paths.get(config)), StandardCharsets.UTF_8);

        } else {

          // read from hadoop
          Configuration conf = job.getConfiguration();
          Path path = new Path(config);
          FileSystem fs = FileSystem.get(path.toUri(), conf);
          FSDataInputStream inputStream = fs.open(path);
          java.util.Scanner scanner = new java.util.Scanner(inputStream).useDelimiter("\\A");
          raw = scanner.hasNext() ? scanner.next() : "";
          fs.close();

        }

        // parse the JSON (need the root before creating the writer)
        JSONParser parser = new JSONParser();
        JSONObject json = (JSONObject) parser.parse(raw);
        JSONObject logging = (JSONObject) json.get("logging");
        if (logging != null) {
          logging_storageAccount = logging.get("storageAccount").toString();
          logging_accountKey = logging.get("accountKey").toString();
          logging_tableName = logging.get("tableName").toString();
        }
        JSONArray cc = (JSONArray) json.get("columns");
        if (cc != null) {
          for (int i = 0; i < cc.size(); i++) {
            JSONObject c = (JSONObject) cc.get(i);
            String name = c.get("name").toString();
            String type = c.get("type").toString();
            String onWrongType = (c.get("onWrongType") != null) ? c.get("onWrongType").toString() : "fail";
            columns.add(new Column(name, type, onWrongType));
          }
        }

      } catch (Exception ex) {
        throw new ExecException(ex);
      }

    }
  }

  public ResourceSchema getSchema(String location, Job job) throws IOException {

    // read the config
    readConfig(location, job);

    // build the output
    List<FieldSchema> list = new ArrayList<FieldSchema>();
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      switch (column.type.toLowerCase()) {
        case "bool":
        case "boolean":
          list.add(new FieldSchema(column.name, DataType.BOOLEAN));
          break;
        case "int":
        case "integer":
          list.add(new FieldSchema(column.name, DataType.INTEGER));
          break;
        case "number":
        case "double":
          list.add(new FieldSchema(column.name, DataType.DOUBLE));
          break;
      }
    }
    return new ResourceSchema(new Schema(list));

  }

  private static boolean empty(final String s) {
    return s == null || s.trim().isEmpty();
  }

	private void log(final String level, final String message) throws IOException {
    if (cloudTable != null) {
      try {
        String partitionKey = instanceId;
        String rowKey = Integer.toString(instanceIndex) + "-" + String.format("%04d", logEntryIndex);
        LogEntity entity = new LogEntity(partitionKey, rowKey, level, message);
        cloudTable.getServiceClient().execute(logging_tableName, TableOperation.insert(entity));
        logEntryIndex++;
      } catch (Exception ex) {
        throw new ExecException(ex); // wrap the exception
      }
    }
	}

  @Override
  public void setLocation(String location, Job job) throws IOException {

    // read the config
    readConfig(location, job);

    // support local and hadoop
    String combiner = location.endsWith("/") ? "" : "/";
    String folder = location.replace("file:", "") + combiner + target;
    if (folder.startsWith("./")) {

      // read from the local file system
      //raw = new String(Files.readAllBytes(Paths.get(config)), StandardCharsets.UTF_8);

    } else {

      // see if there are files to process
      Configuration conf = job.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      Path path = new Path(folder);
      if (fs.exists(path)) {
        RemoteIterator<LocatedFileStatus> i_fs = fs.listFiles(path, true);
        while (i_fs.hasNext()) {
          LocatedFileStatus status = i_fs.next();
          if (status.isFile() && status.getBlockSize() > 0) {
            hasFiles = true;
          }
        }
      }
      fs.close();

    }

    // enable logging to an Azure Table
    UDFContext udfc = UDFContext.getUDFContext();
    if (!udfc.isFrontend() && cloudTable == null && !empty(logging_storageAccount) && !empty(logging_accountKey) && !empty(logging_tableName)) {
      try {

        // create the table
        CloudStorageAccount account = new CloudStorageAccount(new StorageCredentialsAccountAndKey(logging_storageAccount, logging_accountKey), true);
        cloudTable = account.createCloudTableClient().getTableReference(logging_tableName);
				cloudTable.createIfNotExist();

        // make the instance index one higher than the last
        TableQuery<LogEntity> query = TableQuery.from(logging_tableName, LogEntity.class).where("(PartitionKey eq '" + instanceId + "')");
        for (LogEntity entity : cloudTable.getServiceClient().execute(query)) {
          String[] id = entity.getRowKey().split("-");
          int consider = Integer.parseInt(id[0]);
          if (consider > instanceIndex) instanceIndex = consider;
        }
        instanceIndex++;
        
      } catch (Exception ex) {
        throw new ExecException(ex);
      }
    }

    // return either the specified location or the empty location
    if (hasFiles) {
      log("INFO", "/" + target + " found to contain file(s).");
      super.setLocation(folder, job);
    } else {
      log("INFO", "/" + target + " does not exist or is empty.");
      super.setLocation(location, job);
    }

  }

}
