/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.repl.dump;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FileOperations;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.toWriteEntity;

public class TableExport {
  private TableSpec tableSpec;
  private final ReplicationSpec replicationSpec;
  private final Hive db;
  private final HiveConf conf;
  private final Logger logger;
  private final Paths paths;
  private final AuthEntities authEntities = new AuthEntities();

  public TableExport(Paths paths, TableSpec tableSpec, ReplicationSpec replicationSpec,
      Hive db, HiveConf conf, Logger logger)
      throws SemanticException {
    this.tableSpec = (tableSpec != null
        && tableSpec.tableHandle.isTemporary()
        && replicationSpec.isInReplicationScope())
        ? null
        : tableSpec;
    this.replicationSpec = replicationSpec;
    if (this.tableSpec != null && this.tableSpec.tableHandle.isView()) {
      this.replicationSpec.setIsMetadataOnly(true);
    }
    this.db = db;
    this.conf = conf;
    this.logger = logger;
    this.paths = paths;
  }

  public AuthEntities run() throws SemanticException {
    if (tableSpec == null) {
      writeMetaData(null);
    } else if (shouldExport()) {
      //first we should get the correct replication spec before doing metadata/data export
      if (tableSpec.tableHandle.isView()) {
        replicationSpec.setIsMetadataOnly(true);
      }
      PartitionIterable withPartitions = partitions();
      writeMetaData(withPartitions);
      if (!replicationSpec.isMetadataOnly()) {
        writeData(withPartitions);
      }
    }
    return authEntities;
  }

  private PartitionIterable partitions() throws SemanticException {
    try {
      long currentEventId = db.getMSC().getCurrentNotificationEventId().getEventId();
      replicationSpec.setCurrentReplicationState(String.valueOf(currentEventId));
      if (tableSpec.tableHandle.isPartitioned()) {
        if (tableSpec.specType == TableSpec.SpecType.TABLE_ONLY) {
          // TABLE-ONLY, fetch partitions if regular export, don't if metadata-only
          if (replicationSpec.isMetadataOnly()) {
            return null;
          } else {
            return new PartitionIterable(db, tableSpec.tableHandle, null, conf.getIntVar(
                HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX));
          }
        } else {
          // PARTITIONS specified - partitions inside tableSpec
          return new PartitionIterable(tableSpec.partitions);
        }
      } else {
        // Either tableHandle isn't partitioned => null, or repl-export after ts becomes null => null.
        // or this is a noop-replication export, so we can skip looking at ptns.
        return null;
      }
    } catch (Exception e) {
      throw new SemanticException("Error when identifying partitions", e);
    }
  }

  private void writeMetaData(PartitionIterable partitions)
      throws SemanticException {
    try {
      EximUtil.createExportDump(
          paths.exportFileSystem,
          paths.metaDataExportFile(),
          tableSpec == null ? null : tableSpec.tableHandle,
          partitions,
          replicationSpec);
      logger.debug("_metadata file written into " + paths.metaDataExportFile().toString());
    } catch (Exception e) {
      // the path used above should not be used on a second try as each dump request is written to a unique location.
      // however if we want to keep the dump location clean we might want to delete the paths
      throw new SemanticException(
          ErrorMsg.IO_ERROR.getMsg("Exception while writing out the local file"), e);
    }
  }

  private void writeData(PartitionIterable partitions) throws SemanticException {
    try {
      if (tableSpec.tableHandle.isPartitioned()) {
        if (partitions == null) {
          throw new IllegalStateException(
              "partitions cannot be null for partitionTable :" + tableSpec.tableName);
        }
        for (Partition partition : partitions) {
          Path fromPath = partition.getDataLocation();
          // this the data copy
          Path rootDataDumpDir = paths.partitionExportDir(partition.getName());
          new FileOperations(fromPath, rootDataDumpDir, conf).export(replicationSpec);
          authEntities.inputs.add(new ReadEntity(partition));
        }
      } else {
        Path fromPath = tableSpec.tableHandle.getDataLocation();
        //this is the data copy
        new FileOperations(fromPath, paths.dataExportDir(), conf).export(replicationSpec);
        authEntities.inputs.add(new ReadEntity(tableSpec.tableHandle));
      }
      authEntities.outputs.add(toWriteEntity(paths.exportRootDir, conf));
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private boolean shouldExport() throws SemanticException {
    if (replicationSpec.isInReplicationScope()) {
      return !(tableSpec.tableHandle.isTemporary() || tableSpec.tableHandle.isNonNative());
    } else if (tableSpec.tableHandle.isNonNative()) {
      throw new SemanticException(ErrorMsg.EXIM_FOR_NON_NATIVE.getMsg());
    }
    return true;
  }

  /**
   * this class is responsible for giving various paths to be used during export along with root export
   * directory creation.
   */
  public static class Paths {
    private final HiveConf conf;
    public final Path exportRootDir;
    private final FileSystem exportFileSystem;

    /**
     * this is just a helper function interface which creates the path object from the two different
     * constructor's required by users of TableExport.
     */
    interface Function {
      Path path() throws SemanticException;
    }

    public Paths(final String astRepresentationForErrorMsg, final Path dbRoot, final String tblName,
        final HiveConf conf)
        throws SemanticException {
      this(new Function() {
        @Override
        public Path path() throws SemanticException {
          Path tableRoot = new Path(dbRoot, tblName);
          URI exportRootDir = EximUtil.getValidatedURI(conf, tableRoot.toUri().toString());
          validateTargetDir(conf, astRepresentationForErrorMsg, exportRootDir);
          return new Path(exportRootDir);
        }
      }, conf);
    }

    public Paths(Function builder, HiveConf conf)
        throws SemanticException {
      this.conf = conf;
      this.exportRootDir = builder.path();
      try {
        this.exportFileSystem = this.exportRootDir.getFileSystem(conf);
        mkdir(conf, exportFileSystem, exportRootDir);
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }

    public Paths(final String path, final HiveConf conf) throws SemanticException {
      this(new Function() {
        @Override
        public Path path() throws SemanticException {
          return new Path(EximUtil.getValidatedURI(conf, path));
        }
      }, conf);
    }

    private Path partitionExportDir(String partitionName) throws SemanticException {
      return exportDir(new Path(exportRootDir, partitionName));
    }

    private Path metaDataExportFile() {
      return new Path(exportRootDir, EximUtil.METADATA_NAME);
    }

    /**
     * This is currently referring to the export path for the data within a non partitioned table.
     * Partition's data export directory is created within the export semantics of partition.
     */
    private Path dataExportDir() throws SemanticException {
      return exportDir(new Path(exportRootDir, EximUtil.DATA_PATH_NAME));
    }

    private Path exportDir(Path exportDir) throws SemanticException {
      try {
        if (!exportFileSystem.exists(exportDir)) {
          mkdir(conf, exportFileSystem, exportDir);
        }
        return exportDir;
      } catch (IOException e) {
        throw new SemanticException(
            "error while creating directory for partition at " + exportDir, e);
      }
    }

    private static void mkdir(HiveConf conf, final FileSystem fileSystem, final Path path)
        throws IOException {
      if(fileSystem.exists(path)) return;
      Tuple tuple = pathsForPermissions(fileSystem, path);
      HdfsUtils.HadoopFileStatus permissionsToSet = new HdfsUtils.HadoopFileStatus(
          conf, fileSystem,
          tuple.nearestParentAvailable
      );
      // we have to create the directories after we get the tuple for permissions above else wrong
      // directories will be selected.
      fileSystem.mkdirs(path);
      HdfsUtils.setFullFileStatus(conf, permissionsToSet, fileSystem,
          tuple.childNotExistingForNearestParentAvailable, true);
    }

    private static class Tuple {
      private final Path nearestParentAvailable;
      private final Path childNotExistingForNearestParentAvailable;

      Tuple(Path nearestParentAvailable,
          Path childNotExistingForNearestParentAvailable) {
        this.nearestParentAvailable = nearestParentAvailable;
        this.childNotExistingForNearestParentAvailable = childNotExistingForNearestParentAvailable;
      }
    }

    /**
     * for a given path this method finds out the nearest parent that is existing on the file system
     * so that we can inherit permissions from this path. We need to look for the nearest child here
     * as in HdfsUtils.setFullFileStatus we need the root directory on which the permissions need to
     * be applied recursively.
     * This is required for the export / import use case only.
     * for the above we might export data to [some_dir]/partition1=1/partition2=2 where we have to create
     * 'partition1=1/partition2=2'  so tuple will have nearestParentAvailable = [some_dir] and
     * childNotExistingForNearestParentAvailable = '[some_dir]/partition1=1'
     */
    private static Tuple pathsForPermissions(FileSystem fileSystem, Path path)
        throws IOException {
      Path temp = path;
      Path parent = temp.getParent();
      while (parent != null) {
        if (fileSystem.exists(parent)) {
          return new Tuple(parent, temp);
        } else {
          temp = parent;
          parent = temp.getParent();
        }
      }
      throw new IllegalStateException(
          "Could not find a existing parent for path " + path + " to inherit permissions from");
    }

    /**
     * this level of validation might not be required as the root directory in which we dump will
     * be different for each run hence possibility of it having data is not there.
     */
    private static void validateTargetDir(HiveConf conf, String astRepresentationForErrorMsg,
        URI rootDirExportFile)
        throws SemanticException {
      try {
        FileSystem fs = FileSystem.get(rootDirExportFile, conf);
        Path toPath = new Path(rootDirExportFile.getScheme(), rootDirExportFile.getAuthority(),
            rootDirExportFile.getPath());
        try {
          FileStatus tgt = fs.getFileStatus(toPath);
          // target exists
          if (!tgt.isDirectory()) {
            throw new SemanticException(
                astRepresentationForErrorMsg + ": " + "Target is not a directory : "
                    + rootDirExportFile);
          } else {
            FileStatus[] files = fs.listStatus(toPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
            if (files != null && files.length != 0) {
              throw new SemanticException(
                  astRepresentationForErrorMsg + ": " + "Target is not an empty directory : "
                      + rootDirExportFile);
            }
          }
        } catch (FileNotFoundException ignored) {
        }
      } catch (IOException e) {
        throw new SemanticException(astRepresentationForErrorMsg, e);
      }
    }
  }

  public static class AuthEntities {
    public final Set<ReadEntity> inputs = new HashSet<>();
    public final Set<WriteEntity> outputs = new HashSet<>();
  }
}
