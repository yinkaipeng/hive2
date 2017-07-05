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

package org.apache.hadoop.hive.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestFileUtils {

  public static final Logger LOG = LoggerFactory.getLogger(TestFileUtils.class);

  @Test
  public void testCopyWithDistcp() throws IOException {
    Path copySrc = new Path("copySrc");
    Path copyDst = new Path("copyDst");
    HiveConf conf = new HiveConf(TestFileUtils.class);

    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getUri()).thenReturn(URI.create("hdfs:///"));

    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.getLen()).thenReturn(Long.MAX_VALUE);
    when(mockFs.getFileStatus(any(Path.class))).thenReturn(mockFileStatus);

    HadoopShims shims = mock(HadoopShims.class);
    when(shims.runDistCp(Collections.singletonList(copySrc), copyDst, conf)).thenReturn(true);

    Assert.assertTrue(FileUtils.copy(mockFs, copySrc, mockFs, copyDst, false, false, conf, shims));
    verify(shims).runDistCp(Collections.singletonList(copySrc), copyDst, conf);
  }

  @Test
  public void testCopyWithDistCpAs() throws IOException {
    Path copySrc = new Path("copySrc");
    Path copyDst = new Path("copyDst");
    HiveConf conf = new HiveConf(TestFileUtils.class);

    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileStatus.getLen()).thenReturn(Long.MAX_VALUE);

    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getUri()).thenReturn(URI.create("hdfs:///"));
    when(mockFs.getFileStatus(any(Path.class))).thenReturn(mockFileStatus);

    String doAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);

    HadoopShims shims = mock(HadoopShims.class);
    when(shims.runDistCpAs(Collections.singletonList(copySrc), copyDst, conf, doAsUser)).thenReturn(true);
    when(shims.runDistCp(Collections.singletonList(copySrc), copyDst, conf)).thenReturn(false);

    // doAs when asked
    Assert.assertTrue(FileUtils.distCp(mockFs, Collections.singletonList(copySrc), copyDst, true, doAsUser, conf, shims));
    verify(shims).runDistCpAs(Collections.singletonList(copySrc), copyDst, conf, doAsUser);
    // don't doAs when not asked
    Assert.assertFalse(FileUtils.distCp(mockFs, Collections.singletonList(copySrc), copyDst, true, null, conf, shims));
    verify(shims).runDistCp(Collections.singletonList(copySrc), copyDst, conf);
  }
}
