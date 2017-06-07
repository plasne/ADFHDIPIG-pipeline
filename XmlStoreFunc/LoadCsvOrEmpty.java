package input;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.piggybank.storage.CSVLoader;
import org.apache.pig.impl.util.UDFContext;

public class LoadCsvOrEmpty extends CSVLoader {

  public LoadCsvOrEmpty() {
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    System.out.println("!!!!!!!!!!!!! start setLocation");
    UDFContext udfc = UDFContext.getUDFContext();
    if (!udfc.isFrontend()) { // only read on backend
      String folder = location.replace("file:", "");

      System.out.println("!!!!!!!!!!!!! backend");

      // support local and hadoop
      if (folder.startsWith("./")) {

        System.out.println("!!!!!!!!!!!!! wrong");

        // read from the local file system
        //raw = new String(Files.readAllBytes(Paths.get(config)), StandardCharsets.UTF_8);

      } else {

        System.out.println("!!!!!!!!!!!!! process");

        // see if there are files to process
        Configuration conf = udfc.getJobConf();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> i_fs = fs.listFiles(new Path(folder), true);
        System.out.println("!!!!!!!!!!!!! listed");
        while (i_fs.hasNext()) {
          LocatedFileStatus status = i_fs.next();
          if (status.isFile() && status.getBlockSize() > 0) {
            System.out.println("process: " + status.getPath().getName());
          } else {
            System.out.println("ignore: " + status.getPath().getName());
          }
        }
        fs.close();

        System.out.println("!!!!!!!!!!!!! ending");

      }
    }

    System.out.println("!!!!!!!!!!!!! end setLocation");
    super.setLocation(location, job);
  }

}
