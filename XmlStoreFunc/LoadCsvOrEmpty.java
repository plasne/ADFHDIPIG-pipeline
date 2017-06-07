package input;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.piggybank.storage.CSVLoader;

public class LoadCsvOrEmpty extends CSVLoader {

  public LoadCsvOrEmpty() {
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {

    // support local and hadoop
    if (config.startsWith("./")) {

      // read from the local file system
      raw = new String(Files.readAllBytes(Paths.get(config)), StandardCharsets.UTF_8);

    } else {

      // see if there are files to process
      Configuration conf = udfc.getJobConf();
      FileSystem fs = FileSystem.get(conf);
      RemoteIterator<LocatedFileStatus> i_fs = fs.listFiles(new Path(location), true);
      while (i_fs.hasNext()) {
        LocatedFileStatus status = i_fs.next();
        if (status.isFile() && status.getBlockSize() > 0) {
          System.out.println("process: " + status.getPath().getName());
        } else {
          System.out.println("ignore: " + status.getPath().getName());
        }
      }
      fs.close();

    }

    super.setLocation(location, job);
  }

}
