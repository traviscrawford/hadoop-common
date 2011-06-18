package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.AdminOperationsProtocol;
import org.apache.hadoop.util.HostsFileReader;

import java.io.IOException;

/**
 * Manages DFS node membership from files.
 */
public class DFSHostsFileReader extends HostsFileReader {
  public DFSHostsFileReader() throws IOException {
    super();
  }

  @Override
  public void initialize(AdminOperationsProtocol aop) throws IOException {
    setFileNames(getConf().get(DFSConfigKeys.DFS_HOSTS, ""),
        getConf().get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));
    setInitialized(true);
    refresh();
  }
}