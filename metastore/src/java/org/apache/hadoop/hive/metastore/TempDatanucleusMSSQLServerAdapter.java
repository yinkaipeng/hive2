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
package org.apache.hadoop.hive.metastore;

import java.sql.DatabaseMetaData;

import org.datanucleus.store.rdbms.adapter.MSSQLServerAdapter;

/**
 * This is a temporary wrapper around Datanucleus' MSSQLServerAdapter to fix an
 * incorrect syntax issue which Datanucleus generates for range queries for
 * MSSQLServer version 12 and above. MSSQLServerAdapter in Datanucleus generates
 * incorrect syntax for OFFSET-RANGE. For now, we are reverting to the old
 * behavior (<= MSSQLServer version 11) of avoiding the OFFSET-RANGE clause in
 * the query. This class will be removed once the issue is fixed in Datanucleus
 * and Hive upgrades to the version with the fix.
 */
public class TempDatanucleusMSSQLServerAdapter extends MSSQLServerAdapter {

  public TempDatanucleusMSSQLServerAdapter(DatabaseMetaData metadata) {
    super(metadata);
  }

  public String getRangeByLimitEndOfStatementClause(long offset, long count) {
    return "";
  }

}
