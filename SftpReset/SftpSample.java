package com.plasne;

import com.jcraft.jsch.*;
import java.time.*;
import java.time.format.*;

public class SftpSample {
    public static void main(String args[]) {
        JSch jsch = new JSch();
        Session session = null;
        try {

            // arguments
            int offset = Integer.parseInt(args[0]);
            int roundTo = Integer.parseInt(args[1]);
            String input = args[2];
            String output = args[3];
            String host = args[4];
            String user = args[5];
            String pass = args[6];

            // connect via SSH
            session = jsch.getSession(user, host, 22);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(pass);
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
        } finally {
           if (session != null) session.disconnect();
        }
    }
}
