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

    int refreshSec = getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HOSTS_READER_REFRESH_SEC_KEY,
        DFSConfigKeys.DFS_NAMENODE_HOSTS_READER_REFRESH_SEC_DEFAULT);
    if (refreshSec > -1) {
      refresh(aop, refreshSec); // Automagically refresh.
    } else {
      refresh(); // Refresh once.
    }
  }
}