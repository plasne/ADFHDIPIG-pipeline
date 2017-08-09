package com.plasne;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.jcraft.jsch.*;
import java.time.*;
import java.time.format.*;

public class SftpReset {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        // defaults
        private int offset = 0;                             // offset time
        private int roundTo = 0;                            // round to next x minutes
        private String input = "";                          // input folder
        private String output = "";                         // output folder
        private String hostname = "";                       // hostname for SFTP server
        private String username = "";                       // username for SFTP server
        private String password = "";                       // password for SFTP server

        public void configure(JobConf job) {
            offset = job.getInt("offset", 0);
            roundTo = job.getInt("roundTo", -1);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // get all variables
            String line = value.toString();
            String[] keyval = line.split("=");
            switch (keyval[0]) {
                /*
                case "offset":
                    offset = Integer.parseInt(keyval[1]);
                    break;
                case "roundTo":
                    roundTo = Integer.parseInt(keyval[1]);
                    break;
                */
                case "input":
                    input = keyval[1];
                    break;
                case "output":
                    output = keyval[1];
                    break;
                case "hostname":
                    hostname = keyval[1];
                    break;
                case "username":
                    username = keyval[1];
                    break;
                case "password":
                    password = keyval[1];
                    break;
            }

            // determine if there is enough to execute
            Boolean execute = (
                input != null && !input.isEmpty() && 
                output != null && !output.isEmpty() && 
                hostname != null && !hostname.isEmpty() && 
                username != null && !username.isEmpty() && 
                password != null && !password.isEmpty()
            );

            // execute if requested
            if (execute) {
                JSch jsch = new JSch();
                Session session = null;
                try {

                    // connect via SSH
                    session = jsch.getSession(username, hostname, 22);
                    session.setConfig("StrictHostKeyChecking", "no");
                    session.setPassword(password);
                    session.connect();

                    // connect via SFTP
                    Channel channel = session.openChannel("sftp");
                    channel.connect();
                    ChannelSftp sftpChannel = (ChannelSftp) channel;

                    // offset + round
                    LocalDateTime dt_offset = LocalDateTime.now(Clock.systemUTC()).plusMinutes(offset).withSecond(0).withNano(0);
                    LocalDateTime dt_rounded = dt_offset;
                    if (roundTo != 0) {
                        dt_rounded = dt_rounded.plusMinutes( (60 + roundTo - dt_offset.getMinute()) % roundTo);
                    }
                    if (roundTo < 0) {
                        dt_rounded = dt_rounded.plusMinutes( roundTo );
                    }

                    // rename folder
                    String output_ts = dt_rounded.format(DateTimeFormatter.ofPattern(output));
                    sftpChannel.rename(input, output_ts);

                    // create new folder
                    sftpChannel.mkdir(input);

                    // logout
                    sftpChannel.exit();

                } catch (Exception e) {
                    e.printStackTrace();
                    throw new IOException(e);
                } finally {
                    if (session != null) session.disconnect();
                }
            }

        }

    }

    public static class Reduce extends Reducer <Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
            // nothing to do
        }

    }

    public static void main(String[] args) throws Exception {

        // start new configuration
        Configuration conf = new Configuration();

        // read the arguments
        int offset = 0;
        int roundTo = -1;
        String input = "";
        String output = "";
        Boolean roundToWasSet = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch(arg) {
                case "--offset":
                    offset = Integer.parseInt( args[i + 1] );
                    conf.setInt("offset", offset);
                    break;
                case "-r":
                case "--roundTo":
                    roundTo = Integer.parseInt( args[i + 1] );
                    conf.setInt("roundTo", roundTo);
                    roundToWasSet = true;
                    break;
                case "-i":
                case "--input":
                    input = args[i + 1];
                    break;
                case "-o":
                case "--output":
                    output = args[i + 1];
                    break;
            }
        }

        // offset + round for output folder
        if (!roundToWasSet) {
            throw new Exception("-r or --roundTo must be set.");
        }
        LocalDateTime dt_offset = LocalDateTime.now(Clock.systemUTC()).plusMinutes(offset).withSecond(0).withNano(0);
        LocalDateTime dt_rounded = dt_offset;
        if (roundTo != 0) {
            dt_rounded = dt_rounded.plusMinutes( (60 + roundTo - dt_offset.getMinute()) % roundTo);
        }
        if (roundTo < 0) {
            dt_rounded = dt_rounded.plusMinutes( roundTo );
        }
        String output_ts = dt_rounded.format(DateTimeFormatter.ofPattern(output));

        // create the job
        Job job = new Job(conf);
        job.setJobName("sftpreset");
        job.setJarByClass(SftpReset.class);  //job.setJar("SftpReset.jar");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(SftpReset.Map.class);
        job.setCombinerClass(SftpReset.Reduce.class);
        job.setReducerClass(SftpReset.Reduce.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output_ts));
        job.waitForCompletion(true);

    }

}
