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
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.mapreduce.Job;
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
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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

  boolean hasFiles = false;
  String target;
  String config;
  private ArrayList<Column> columns = new ArrayList<Column>();
  BufferedWriter log;

  public LoadCsvOrEmpty(String target, String config) {
    this.target = target;
    this.config = config;
  }

  @Override
  public Tuple getNext() throws IOException {
    if (hasFiles) {
      Tuple t;
      boolean skipped = false;
      do {
        log.write("read\n");
        t = super.getNext();
        skipped = false;
        if (t != null) {

          // verify number of columns
          int size = columns.size();
          if (t.size() != size) {
            throw new ExecException("size " + t.size() + " vs " + size, 2200, PigException.BUG);
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
                    if (column.onWrongType.equals("skip")) {
                      log.write("skip\n");
                      skipped = true;
                    } else {
                      throw new ExecException("expected boolean but saw " + DataType.findTypeName(type), 2201, PigException.BUG);
                    }
                  }
                  break;
                case "int":
                case "integer":
                  try {
                    t.set(i, DataType.toInteger(value));
                  } catch (Exception ex) {
                    if (column.onWrongType.equals("skip")) {
                      log.write("skip\n");
                      skipped = true;
                    } else {
                      throw new ExecException("expected integer but saw " + DataType.findTypeName(type) + " onWrongType: " + column.onWrongType, 2201, PigException.BUG);
                    }
                  }
                  break;
                case "number":
                case "double":
                  try {
                    t.set(i, DataType.toDouble(value));
                  } catch (Exception ex) {
                    if (column.onWrongType.equals("skip")) {
                      log.write("skip\n");
                      skipped = true;
                    } else {
                      throw new ExecException("expected double but saw " + DataType.findTypeName(type), 2201, PigException.BUG);
                    }
                  }
                  break;
              }
            }
          }

        }
      } while (skipped);
      if (t == null) {
        log.write("close file\n");
      }
      return t;

    } else {
      return null;
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

      // open the log file
      if (log == null) {
        String combiner = location.endsWith("/") ? "" : "/";
        String folder = location.replace("file:", "") + combiner + "input.log";

        Configuration conf = new Configuration();
        Path path = new Path(folder);
        FileSystem hdfs = FileSystem.get(path.toUri(), conf);
        if (hdfs.exists(path)) hdfs.delete(path, true); 
        OutputStream os = hdfs.create(path);
        log = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        hdfs.close();
        
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

    // return either the specified location or the empty location
    if (hasFiles) {
      super.setLocation(folder, job);
    } else {
      super.setLocation(location, job);
    }

  }

}