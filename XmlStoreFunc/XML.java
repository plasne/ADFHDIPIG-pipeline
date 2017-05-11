package output;

import java.util.Scanner;
import java.util.Map;
import java.util.Iterator;
import java.util.Properties;
import java.util.ArrayList;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.io.DataOutputStream;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.XMLStreamException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataBag;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.UDFContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class XMLOutputFormat<T1, T2> extends TextOutputFormat<T1, T2> {

  private java.lang.String root;
  private ArrayList<java.lang.String> pre;
  private ArrayList<java.lang.String> post;

  public XMLOutputFormat(java.lang.String root, ArrayList<java.lang.String> pre, ArrayList<java.lang.String> post, ArrayList<java.lang.String> onclose) {
    this.root = root;
    this.pre = pre;
    this.post = post;
    this.onclose = onclose;
  }

  @Override
  public RecordWriter<T1, T2> getRecordWriter(TaskAttemptContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    Path file = getDefaultWorkFile(job, ".xml");
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream out = fs.create(file, false);
    return new XMLRecordWriter<T1, T2>(out, file.toString(), root, pre, post, onclose);
  }

  protected static class XMLRecordWriter<T1, T2> extends RecordWriter<T1, T2> {

    private DataOutputStream out;
    private java.lang.String filename;
    private java.lang.String root;
    private ArrayList<java.lang.String> pre;
    private ArrayList<java.lang.String> post;
    private ArrayList<java.lang.String> onclose;

    public XMLRecordWriter(DataOutputStream out, java.lang.String filename, java.lang.String root, ArrayList<java.lang.String> pre, ArrayList<java.lang.String> post, ArrayList<java.lang.String> onclose) throws IOException {

      // local variables
      this.out = out;
      this.filename = filename;
      this.root = root;
      this.pre = pre;
      this.post = post;
      this.onclose = onclose;

      // write any headers
      for (int i = 0; i < pre.size(); i++) {
        java.lang.String line = (java.lang.String) pre.get(i);
        out.writeBytes(line);
      }
      out.writeBytes("<" + root + ">");

    }

    public synchronized void write(T1 key, T2 value) throws IOException {
      out.writeBytes(value.toString());
    }

    public synchronized void close(TaskAttemptContext job) throws IOException {

      // write any footers
      try {
        out.writeBytes("</" + root + ">");
        for (int i = 0; i < post.size(); i++) {
          java.lang.String line = (java.lang.String) post.get(i);
          out.writeBytes(line);
        }
      } finally {
        out.close();
      }

      // run onclose events
      for (int j = 0; j < onclose.size(); j++) {
        java.lang.String cmd = (java.lang.String) onclose.get(j);
        cmd = cmd.replace("{file}", filename);
        System.out.println("------------------------------");
        System.out.println(cmd);
        System.out.println("------------------------------");
        Runtime.getRuntime().exec(cmd);
      }

    }

  }

}

class Processor {
  public java.lang.String column;
  public java.lang.String type;
  public java.lang.String node;
  public ArrayList<java.lang.String> children = new ArrayList<java.lang.String>();

  public Processor(java.lang.String column, java.lang.String type, java.lang.String node, JSONArray children) {
    this.column = column;
    this.type = type;
    this.node = node;
    for (int i = 0; i < children.size(); i++) {
      java.lang.String child = (java.lang.String) children.get(i);
      this.children.add(child);
    }
  }

}

public class XML extends StoreFunc {

  private RecordWriter writer = null;
  private String udfcSignature = null;
  private ResourceFieldSchema[] fields = null;

  private StringWriter str;
  private XMLOutputFactory xof;
  private XMLStreamWriter xml;

  private java.lang.String config;
  private java.lang.String root;
  private java.lang.String entry;
  private ArrayList<Processor> processors = new ArrayList<Processor>();

  public XML(java.lang.String config) {
    this.config = config;
  }

  public XML(java.lang.String root, java.lang.String entry) {
    this.root = root;
    this.entry = entry;
  }

  public XML() {
    this.root = "root";
    this.entry = "entry";
  }

  @Override
  public void putNext(Tuple t) throws IOException {
    try {

      // create the factory for the XML
      StringWriter str = new StringWriter();
      XMLOutputFactory xof = XMLOutputFactory.newInstance();
      XMLStreamWriter xml = xof.createXMLStreamWriter(str);

      // create a new entry
      xml.writeStartElement(entry);

      // write each element
      for (int i = 0; i < fields.length; i++) {
        ResourceFieldSchema field = fields[i];
        java.lang.String fieldName = field.getName();

        // find the processor
        Processor processor = null;
        for (int j = 0; j < processors.size(); j++) {
          Processor potential = processors.get(j);
          if (potential.column.equals(fieldName)) processor = potential;
        }
        if (processor != null) {
          switch (processor.type) {

            // scale processor (qty,value;qty,value;qty,value)
            case "scale":
              java.lang.String raw = (java.lang.String) t.get(i);
              java.lang.String body = raw.substring(1, raw.length() - 1);
              java.lang.String[] segments = body.split(";");
              for (java.lang.String segment : segments) {
                xml.writeStartElement(processor.node);
                java.lang.String[] columns = segment.split(",");
                for (int k = 0; k < processor.children.size(); k++) {
                  java.lang.String child = (java.lang.String) processor.children.get(k);
                  xml.writeStartElement(child);
                  java.lang.String v = (java.lang.String) columns[k];
                  xml.writeCharacters(v);
                  xml.writeEndElement();
                }
                xml.writeEndElement();
              }
              break;

          }
        } else {

          // write without a processor
          Object value = t.get(i);
          if (value != null) {
            xml.writeStartElement(fieldName);
            xml.writeCharacters(fieldToString(field, value));
            xml.writeEndElement();
          }

        }

      }

      // close out the entry
      xml.writeEndElement();

      // write to the RecordWriter
      xml.flush();
      xml.close();
      writer.write(null, str.getBuffer().toString());
      str.close();

    } catch (Exception e) {
      throw new ExecException(e);
    }
  }

  private String fieldToString(ResourceFieldSchema field, Object value) throws IOException {

    switch (field.getType()) {

      case DataType.NULL:
        return "";

      case DataType.BOOLEAN:
        return ((Boolean)value).toString();

      case DataType.INTEGER:
        return ((Integer)value).toString();

      case DataType.LONG:
        return ((Long)value).toString();

      case DataType.FLOAT:
        return ((Float)value).toString();

      case DataType.DOUBLE:
        return ((Double)value).toString();

      case DataType.BYTEARRAY:
        byte[] b = ((DataByteArray)value).get();
        Text text = new Text(b);
        return text.toString();

      case DataType.CHARARRAY:
        return (java.lang.String)value;

      case DataType.MAP:
      case DataType.TUPLE:
      case DataType.BAG:
        throw new ExecException("MAP, TUPLE, and BAG are not supported.", 2107, PigException.BUG);

      default:
        throw new ExecException("Could not determine data type of field: " + field, 2108, PigException.BUG);

    }
  }

  @Override
  public OutputFormat getOutputFormat() throws IOException {
    ArrayList<java.lang.String> pre = new ArrayList<java.lang.String>();
    ArrayList<java.lang.String> post = new ArrayList<java.lang.String>();
    try {
      UDFContext udfc = UDFContext.getUDFContext();
      if (config != null && !udfc.isFrontend()) { // only read on backend
        String raw;
        
        // support local and hadoop
        if (config.startsWith("./")) {

          // read from the local file system
          raw = new String(Files.readAllBytes(Paths.get(config)), StandardCharsets.UTF_8);

        } else {

          // read from hadoop
          Configuration conf = udfc.getJobConf();
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
        root = json.get("root").toString();
        entry = json.get("entry").toString();
        JSONArray procarray = (JSONArray) json.get("processors");
        if (procarray != null) {
          for (int i = 0; i < procarray.size(); i++) {
            JSONObject proc = (JSONObject) procarray.get(i);
            java.lang.String column = proc.get("column").toString();
            java.lang.String type = proc.get("type").toString();
            java.lang.String node = proc.get("node").toString();
            JSONArray children = (JSONArray) proc.get("children");
            processors.add(new Processor(column, type, node, children));
          }
        }
        JSONArray pre_section = (JSONArray) json.get("pre");
        if (pre_section != null) {
          for (int i = 0; i < pre_section.size(); i++) {
            java.lang.String line = (java.lang.String) pre_section.get(i);
            pre.add(line);
          }
        }
        JSONArray post_section = (JSONArray) json.get("post");
        if (post_section != null) {
          for (int i = 0; i < post_section.size(); i++) {
            java.lang.String line = (java.lang.String) post_section.get(i);
            post.add(line);
          }
        }
        JSONArray onclose_section = (JSONArray) json.get("onclose");
        if (onclose_section != null) {
          for (int i = 0; i < onclose_section.size(); i++) {
            java.lang.String line = (java.lang.String) onclose_section.get(i);
            onclose.add(line);
          }
        }

      }

    } catch (Exception ex) {
      throw new ExecException(ex);
    }
    return new XMLOutputFormat<WritableComparable, Text>(root, pre, post, onclose);
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    udfcSignature = signature;
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    UDFContext udfc = UDFContext.getUDFContext();
    Properties p = udfc.getUDFProperties(this.getClass(), new String[] { udfcSignature });
    p.setProperty("pig.xmlstorage.schema", s.toString());
  }

  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    try {
      this.writer = writer;

      // read the schema
      UDFContext udfc = UDFContext.getUDFContext();
      Properties p = udfc.getUDFProperties(this.getClass(), new String[] { udfcSignature });
      String s = p.getProperty("pig.xmlstorage.schema");
      if (s == null) throw new ExecException("Could not find schema in UDF context.");
      ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(s));
      fields = schema.getFields();

    } catch (Exception ex) {
      throw new ExecException(ex);
    }
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

}
