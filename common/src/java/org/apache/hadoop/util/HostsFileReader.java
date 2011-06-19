/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Keeps track of hosts included/excluded from the cluster.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public abstract class HostsFileReader implements HostsReader {
  private static final Log LOG = LogFactory.getLog(HostsFileReader.class);
  private Configuration conf;
  private Set<String> includes;
  private Set<String> excludes;
  private String includesFile;
  private String excludesFile;
  private boolean initialized = false;

  public HostsFileReader() throws IOException {
    includes = new HashSet<String>();
    excludes = new HashSet<String>();
  }

  public synchronized Set<String> getHosts() {
    Preconditions.checkState(initialized);
    return includes;
  }

  public synchronized Set<String> getExcludedHosts() {
    Preconditions.checkState(initialized);
    return excludes;
  }

  /**
   * Refresh once right now.
   * @return true if refresh was successful, otherwise false
   * @throws IOException if unable to read hosts files
   */
  public synchronized boolean refresh() throws IOException {
    Preconditions.checkState(initialized);
    boolean updated = false;
    LOG.info("Refreshing hosts lists: includes=" + includesFile
        + " excludes=" + excludesFile);
    if (includesFile != null) {
      Set<String> newIncludes = new HashSet<String>();
      readFileToSet(includesFile, newIncludes);
      if (!includes.equals(newIncludes)) {
        includes = newIncludes;
        updated = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated includes: " + includes);
        }
      }
    }
    if (excludesFile != null) {
      Set<String> newExcludes = new HashSet<String>();
      readFileToSet(excludesFile, newExcludes);
      if (!excludes.equals(newExcludes)) {
        excludes = newExcludes;
        updated = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated excludes: " + excludes);
        }
      }
    }
    return updated;
  }

  /**
   * Automatically refresh every period of time, starting now.
   * @param aop what to refresh
   * @param refreshSec number of seconds between refreshes
   * @return true if refresh was successful, otherwise false
   * @throws IOException if unable to read hosts files
   */
  public synchronized boolean refresh(AdminOperationsProtocol aop, int refreshSec) throws IOException {
    Preconditions.checkState(initialized);
    LOG.info("Enabling automagic refreshNodes every " + refreshSec + " seconds.");
    Timer refreshTimer = new Timer();
    refreshTimer.schedule(new HostsFileRefreshTask(aop), refreshSec, refreshSec * 1000);
    return refresh();
  }

  public void setInitialized(boolean val) {
    initialized = val;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  private void readFileToSet(String filename, Set<String> set) throws IOException {
    File file = new File(filename);
    if (!file.exists()) {
      return;
    }
    FileInputStream fis = new FileInputStream(file);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (String node : nodes) {
            if (node.trim().startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!node.equals("")) {
              set.add(node);  // might need to add canonical name
            }
          }
        }
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }
  }

  protected synchronized void setFileNames(String includes, String excludes) {
    if (includes.equals("")) {
      LOG.info("Not using a hosts include file as its value is unspecified.");
    } else {
      LOG.info("Setting includes file to " + includes);
      includesFile = includes;
    }

    if (excludes.equals("")) {
      LOG.info("Not using a hosts exclude file as its value is unspecified.");
    } else {
      LOG.info("Setting excludes file to " + excludes);
      excludesFile = excludes;
    }
  }

  /**
   * Task for periodically refreshing hosts. Each period of time
   * includes/excludes files are checked for changes. refreshNodes
   * is called when changes are detected.
   */
  public class HostsFileRefreshTask extends TimerTask {
    private AdminOperationsProtocol aop;

    public HostsFileRefreshTask(AdminOperationsProtocol aop) {
      this.aop = aop;
    }

    public void run() {
      try {
        if (refresh()) {
          aop.refreshNodes();
        }
      } catch (IOException e) {
        LOG.error("Failed refreshing nodes!", e);
      }
    }
  }
}