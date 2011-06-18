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
package org.apache.hadoop.hdfs.util;

import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.AdminOperationsProtocol;
import org.apache.hadoop.util.HostsFileReader;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

import static org.junit.Assert.*;

public class TestDFSHostsFileReader {
  private final String HOSTS_TEST_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();
  private File excludesFile = new File(HOSTS_TEST_DIR, "dfs.exclude");
  private File includesFile = new File(HOSTS_TEST_DIR, "dfs.include");
  private Configuration conf = new Configuration();

  @Before
  public void setUp() throws Exception {
    this.conf.set(DFSConfigKeys.DFS_HOSTS, includesFile.getAbsolutePath());
    this.conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludesFile.getAbsolutePath());

    excludesFile.deleteOnExit();
    includesFile.deleteOnExit();
  }

  /*
   * 1.Create dfs.exclude,dfs.include file
   * 2.Write host names per line
   * 3.Write comments starting with #
   * 4.Close file
   * 5.Compare if number of hosts reported by HostsFileReader
   *   are equal to the number of hosts written
   */
  @Test
  public void testHostsFileReader() throws Exception {

    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.write("somehost1\n");
    efw.write("#This-is-comment\n");
    efw.write("somehost2\n");
    efw.write("somehost3 # host3\n");
    efw.write("somehost4\n");
    efw.write("somehost4 somehost5\n");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.write("somehost1\n");
    ifw.write("somehost2\n");
    ifw.write("somehost3\n");
    ifw.write("#This-is-comment\n");
    ifw.write("somehost4 # host4\n");
    ifw.write("somehost4 somehost5\n");
    ifw.close();

    HostsFileReader hostsFileReader = new DFSHostsFileReader();
    hostsFileReader.setConf(conf);
    AdminOperationsProtocol mockedAop = mock(AdminOperationsProtocol.class);
    hostsFileReader.initialize(mockedAop);

    int includesLen = hostsFileReader.getHosts().size();
    int excludesLen = hostsFileReader.getExcludedHosts().size();

    assertEquals(5, includesLen);
    assertEquals(5, excludesLen);

    assertTrue(hostsFileReader.getHosts().contains("somehost5"));
    assertFalse(hostsFileReader.getHosts().contains("host3"));

    assertTrue(hostsFileReader.getExcludedHosts().contains("somehost5"));
    assertFalse(hostsFileReader.getExcludedHosts().contains("host4"));
  }

  /*
   * Test for null file
   */
  @Test
  public void testHostFileReaderWithNull() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.close();
    ifw.close();

    HostsFileReader hostsFileReader = new DFSHostsFileReader();
    hostsFileReader.setConf(conf);
    AdminOperationsProtocol mockedAop = mock(AdminOperationsProtocol.class);
    hostsFileReader.initialize(mockedAop);

    int includesLen = hostsFileReader.getHosts().size();
    int excludesLen = hostsFileReader.getExcludedHosts().size();

    // TestCase1: Check if lines beginning with # are ignored
    assertEquals(0, includesLen);
    assertEquals(0, excludesLen);

    // TestCase2: Check if given host names are reported by getHosts and
    // getExcludedHosts
    assertFalse(hostsFileReader.getHosts().contains("somehost5"));

    assertFalse(hostsFileReader.getExcludedHosts().contains("somehost5"));
  }

  /*
   * Check if only comments can be written to hosts file
   */
  @Test
  public void testHostFileReaderWithCommentsOnly() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.close();

    HostsFileReader hostsFileReader = new DFSHostsFileReader();
    hostsFileReader.setConf(conf);
    AdminOperationsProtocol mockedAop = mock(AdminOperationsProtocol.class);
    hostsFileReader.initialize(mockedAop);

    int includesLen = hostsFileReader.getHosts().size();
    int excludesLen = hostsFileReader.getExcludedHosts().size();

    assertEquals(0, includesLen);
    assertEquals(0, excludesLen);

    assertFalse(hostsFileReader.getHosts().contains("somehost5"));

    assertFalse(hostsFileReader.getExcludedHosts().contains("somehost5"));
  }

  /*
   * Test if spaces are allowed in host names
   */
  @Test
  public void testHostFileReaderWithSpaces() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.write("   somehost somehost2");
    efw.write("   somehost3 # somehost4");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.write("   somehost somehost2");
    ifw.write("   somehost3 # somehost4");
    ifw.close();

    HostsFileReader hostsFileReader = new DFSHostsFileReader();
    hostsFileReader.setConf(conf);
    AdminOperationsProtocol mockedAop = mock(AdminOperationsProtocol.class);
    hostsFileReader.initialize(mockedAop);

    int includesLen = hostsFileReader.getHosts().size();
    int excludesLen = hostsFileReader.getExcludedHosts().size();

    assertEquals(3, includesLen);
    assertEquals(3, excludesLen);

    assertTrue(hostsFileReader.getHosts().contains("somehost3"));
    assertFalse(hostsFileReader.getHosts().contains("somehost5"));
    assertFalse(hostsFileReader.getHosts().contains("somehost4"));

    assertTrue(hostsFileReader.getExcludedHosts().contains("somehost3"));
    assertFalse(hostsFileReader.getExcludedHosts().contains("somehost5"));
    assertFalse(hostsFileReader.getExcludedHosts().contains("somehost4"));
  }

  /*
   * Test if spaces , tabs and new lines are allowed
   */
  @Test
  public void testHostFileReaderWithTabs() throws Exception {
    FileWriter efw = new FileWriter(excludesFile);
    FileWriter ifw = new FileWriter(includesFile);

    efw.write("#DFS-Hosts-excluded\n");
    efw.write("     \n");
    efw.write("   somehost \t somehost2 \n somehost4");
    efw.write("   somehost3 \t # somehost5");
    efw.close();

    ifw.write("#Hosts-in-DFS\n");
    ifw.write("     \n");
    ifw.write("   somehost \t  somehost2 \n somehost4");
    ifw.write("   somehost3 \t # somehost5");
    ifw.close();

    HostsFileReader hostsFileReader = new DFSHostsFileReader();
    hostsFileReader.setConf(conf);
    AdminOperationsProtocol mockedAop = mock(AdminOperationsProtocol.class);
    hostsFileReader.initialize(mockedAop);

    int includesLen = hostsFileReader.getHosts().size();
    int excludesLen = hostsFileReader.getExcludedHosts().size();

    assertEquals(4, includesLen);
    assertEquals(4, excludesLen);

    assertTrue(hostsFileReader.getHosts().contains("somehost2"));
    assertFalse(hostsFileReader.getHosts().contains("somehost5"));

    assertTrue(hostsFileReader.getExcludedHosts().contains("somehost2"));
    assertFalse(hostsFileReader.getExcludedHosts().contains("somehost5"));
  }
}