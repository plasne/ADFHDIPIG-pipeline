package input;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.piggybank.storage.CSVLoader;

class LoadCsvOrEmpty extends CSVLoader {

  public LoadCsvOrEmpty(String delim) {
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    System.out.println("location: " + location);
    super.setLocation(location, job);
  }

}
