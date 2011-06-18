package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.util.AdminOperationsProtocol;
import org.apache.hadoop.util.HostsFileReader;

import java.io.IOException;

/**
 * Manages MR node membership from files.
 */
public class MRHostsFileReader extends HostsFileReader {
  public MRHostsFileReader() throws IOException {
    super();
  }

  @Override
  public void initialize(AdminOperationsProtocol aop) throws IOException {
    setFileNames(getConf().get(JTConfig.JT_HOSTS_FILENAME, ""),
        getConf().get(JTConfig.JT_HOSTS_EXCLUDE_FILENAME, ""));
    setInitialized(true);

    int refreshSec = getConf().getInt(JTConfig.JT_HOSTS_READER_REFRESH_SEC_KEY,
        JTConfig.JT_HOSTS_READER_REFRESH_SEC_DEFAULT);
    if (refreshSec > -1) {
      refresh(aop, refreshSec); // Automagically refresh.
    } else {
      refresh(); // Refresh once.
    }
  }
}