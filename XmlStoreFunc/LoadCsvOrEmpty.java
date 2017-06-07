package input;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.piggybank.storage.CSVLoader;

public class LoadCsvOrEmpty extends CSVLoader {

  public LoadCsvOrEmpty() {
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    System.out.println("location: " + location);
    super.setLocation(location, job);
  }

}
