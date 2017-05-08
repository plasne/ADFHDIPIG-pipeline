package output;

import java.util.Scanner;
import java.util.Map;
import java.util.Iterator;
import java.util.Properties;
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

  public XMLOutputFormat(java.lang.String root) {
    this.root = root;
  }

  @Override
  public RecordWriter<T1, T2> getRecordWriter(TaskAttemptContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    Path file = getDefaultWorkFile(job, ".xml");
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream out = fs.create(file, false);
    return new XMLRecordWriter<T1, T2>(out, root);
  }

  protected static class XMLRecordWriter<T1, T2> extends RecordWriter<T1, T2> {

    private DataOutputStream out;
    private java.lang.String root;

    public XMLRecordWriter(DataOutputStream out, java.lang.String root) throws IOException {
      this.out = out;
      this.root = root;
      out.writeBytes("<" + root + ">");
    }

    public synchronized void write(T1 key, T2 value) throws IOException {
       out.writeBytes(value.toString());
    }

    public synchronized void close(TaskAttemptContext job) throws IOException {
      try {
        out.writeBytes("</" + root + ">");
      } finally {
        out.close();
      }
    }

  }

}

class Processor {
  public java.lang.String column;
  public java.lang.String type;

  public Processor(java.lang.String column, java.lang.String type) {
    this.column = column;
    this.type = type;
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
          //xml.writeStartElement("Scale");

          //xml.writeEndElement();

        Object value = t.get(i);
        if (value != null) {
          xml.writeStartElement(fieldName);
          xml.writeCharacters(fieldToString(field, value));
          xml.writeEndElement();
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
    try {
      UDFContext udfc = UDFContext.getUDFContext();
      if (!udfc.isFrontend()) { // only read on backend
     
        // read the contents of the config file
        Configuration conf = udfc.getJobConf();
        Path path = new Path(config);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataInputStream inputStream = fs.open(path);
        java.util.Scanner scanner = new java.util.Scanner(inputStream).useDelimiter("\\A");
        String raw = scanner.hasNext() ? scanner.next() : "";
        fs.close();

        // parse the JSON (need the root before creating the writer)
        JSONParser parser = new JSONParser();
        JSONObject json = (JSONObject) parser.parse(raw);
        root = json.get("root").toString();
        entry = json.get("entry").toString();
        JSONArray procarray = (JSONArray) json.get("processors");
        for (int i = 0; i < procarray.length(); i++) {
          JSONObject proc = (JSONObject) procarray.get(i);
          processors.add(new Processor(proc.get("column").toString(), proc.get("type").toString()));
        }

      }

    } catch (Exception ex) {
      throw new ExecException(ex);
    }
    return new XMLOutputFormat<WritableComparable, Text>(root);
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
