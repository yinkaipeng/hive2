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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestTxnNoBuckets extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnNoBuckets.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnNoBuckets.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Rule
  public TestName testName = new TestName();
  @Override
  String getTestDataDir() {
    return TEST_DATA_DIR;
  }
  @Override
  @Before
  public void setUp() throws Exception {
    setUpInternal();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
  }

  private void checkExpected(List<String> rs, String[][] expected, String msg) {
    LOG.warn(testName.getMethodName() + ": read data(" + msg + "): ");
    for (String s : rs) {
      LOG.warn(s);
    }
    Assert.assertEquals(testName.getMethodName() + ": " + msg, expected.length, rs.size());
    //verify data and layout
    for (int i = 0; i < expected.length; i++) {
      Assert.assertTrue("Actual line " + i + " bc: " + rs.get(i), rs.get(i).startsWith(expected[i][0]));
      Assert.assertTrue("Actual line(file) " + i + " bc: " + rs.get(i), rs.get(i).endsWith(expected[i][1]));
    }
  }
  /**
   * HIVE-17900
   */
  @Test
  public void testCompactStatsGather() throws Exception {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
      hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
      hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T(a int, b int) partitioned by (p int, q int) " +
      "CLUSTERED BY(a) INTO 1 BUCKETS stored as orc TBLPROPERTIES ('transactional'='true')");

    int[][] targetVals = {{4, 1, 1}, {4, 2, 2}, {4, 3, 1}, {4, 4, 2}};
    //we only recompute stats after major compact if they existed before
    runStatementOnDriver("insert into T partition(p=1,q) " + makeValuesClause(targetVals));
    runStatementOnDriver("analyze table T  partition(p=1) compute statistics for columns");

    IMetaStoreClient hms = Hive.get().getMSC();
    List<String> partNames = new ArrayList<>();
    partNames.add("p=1/q=2");
    List<String> colNames = new ArrayList<>();
    colNames.add("a");
    Map<String, List<ColumnStatisticsObj>> map = hms.getPartitionColumnStatistics("default",
      "T", partNames, colNames);
    Assert.assertEquals(4, map.get(partNames.get(0)).get(0).getStatsData().getLongStats().getHighValue());


    int[][] targetVals2 = {{5, 1, 1}, {5, 2, 2}, {5, 3, 1}, {5, 4, 2}};
    runStatementOnDriver("insert into T partition(p=1,q) " + makeValuesClause(targetVals2));

    String query = "select ROW__ID, p, q, a, b, INPUT__FILE__NAME from T order by p, q, a, b";
    List<String> rs = runStatementOnDriver(query);

    //run major compaction
    runStatementOnDriver("alter table T partition(p=1,q=2) compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);

    query = "select ROW__ID, p, q, a, b, INPUT__FILE__NAME from T order by p, q, a, b";
    rs = runStatementOnDriver(query);

    //now check that stats were updated
    map = hms.getPartitionColumnStatistics("default","T", partNames, colNames);
    Assert.assertEquals("", 5, map.get(partNames.get(0)).get(0).getStatsData().getLongStats().getHighValue());
  }
}

