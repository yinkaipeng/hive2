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

package org.apache.hadoop.hive.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class HdfsUtils {

  // TODO: this relies on HDFS not changing the format; we assume if we could get inode ID, this
  //       is still going to work. Otherwise, file IDs can be turned off. Later, we should use
  //       as public utility method in HDFS to obtain the inode-based path.
  private static String HDFS_ID_PATH_PREFIX = "/.reserved/.inodes/";
  static Logger LOG = LoggerFactory.getLogger("shims.HdfsUtils");

  public static Path getFileIdPath(
      FileSystem fileSystem, Path path, long fileId) {
    return (fileSystem instanceof DistributedFileSystem)
        ? new Path(HDFS_ID_PATH_PREFIX + fileId) : path;
  }

  public static void setFullFileStatus(Configuration conf, HdfsUtils.HadoopFileStatus sourceStatus,
      FileSystem fs, Path target, boolean recursion) throws IOException {
    setFullFileStatus(conf, sourceStatus, null, fs, target, recursion);
  }

  public static void setFullFileStatus(Configuration conf, HdfsUtils.HadoopFileStatus sourceStatus,
    String targetGroup, FileSystem fs, Path target, boolean recursion) throws IOException {
    FileStatus fStatus= sourceStatus.getFileStatus();
    String group = fStatus.getGroup();
    boolean aclEnabled = Objects.equal(conf.get("dfs.namenode.acls.enabled"), "true");
    FsPermission sourcePerm = fStatus.getPermission();
    List<AclEntry> aclEntries = null;
    if (aclEnabled) {
      if (sourceStatus.getAclEntries() != null) {
        LOG.trace(sourceStatus.aclStatus.toString());
        aclEntries = new ArrayList<>(sourceStatus.getAclEntries());
        List<AclEntry> defaultAclEntries = extractBaseDefaultAclEntries(aclEntries);
        
        if ( defaultAclEntries.size() > 0) {
          removeBaseAclEntries(aclEntries);
        
          //remove base acl entries if there is a default acl set for that named user|group
          List<AclEntry> temp = new ArrayList<AclEntry>();
          for(AclEntry entry : aclEntries) {
            if (defaultAclEntries.contains(entry)) {
              for (AclEntry deleteEntry : aclEntries){
                if (deleteEntry.getType().equals(entry.getType()) &&
                    deleteEntry.getName().equals(entry.getName())){
                  temp.add(deleteEntry);
                }
              }
            }
          }
          if (temp.size() > 0){
            aclEntries.removeAll(temp);
          }
        
          //set directory's ACL entries based on parent's DEFAULT entries
          for(AclEntry entry: defaultAclEntries) {
            if (entry.getName() != null){
              aclEntries.add(newAclEntry(AclEntryScope.ACCESS, entry.getType(), entry.getName(), entry.getPermission()));
            }
            else {
              aclEntries.add(newAclEntry(AclEntryScope.ACCESS, entry.getType(), entry.getPermission()));
            }
          }
        } else {
          //the ACL api's also expect the tradition user/group/other permission in the form of ACL
          sourcePerm = sourceStatus.getFileStatus().getPermission();
        
          //this is default permissions set on a directory without any ACLs
          if (aclEntries.size() == 0) {
            aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.GROUP, sourcePerm.getGroupAction()));
          }
        
          aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.USER, sourcePerm.getUserAction()));
          aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.OTHER, sourcePerm.getOtherAction()));
        }
      }
    }

    if (recursion) {
      //use FsShell to change group, permissions, and extended ACL's recursively
      FsShell fsShell = new FsShell();
      fsShell.setConf(conf);

      try {
        //If there is no group of a file, no need to call chgrp
        if (group != null && !group.isEmpty()) {
          run(fsShell, new String[]{"-chgrp", "-R", group, target.toString()});
        }
        if (aclEnabled) {
          if (null != aclEntries) {
            //Attempt extended Acl operations only if its enabled, 8791but don't fail the operation regardless.
            try {
              //construct the -setfacl command
              String aclEntry = Joiner.on(",").join(aclEntries);
              run(fsShell, new String[]{"-setfacl", "-R", "--set", aclEntry, target.toString()});

            } catch (Exception e) {
              LOG.info("Skipping ACL inheritance: File system for path " + target + " " +
                  "does not support ACLs but dfs.namenode.acls.enabled is set to true. ");
              LOG.debug("The details are: " + e, e);
            }
          }
        } else {
          String permission = Integer.toString(sourcePerm.toShort(), 8);
          run(fsShell, new String[]{"-chmod", "-R", permission, target.toString()});
        }
      } catch (Exception e) {
        throw new IOException("Unable to set permissions of " + target, e);
      }
    } else {
      if (group != null && !group.isEmpty()) {
        if (targetGroup == null ||
            !group.equals(targetGroup)) {
          fs.setOwner(target, null, group);
        }
      }
      if (aclEnabled) {
        if (null != aclEntries) {
          fs.setAcl(target, aclEntries);
        }
      } else {
        fs.setPermission(target, sourcePerm);
      }
    }
  }

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope
   *          AclEntryScope scope of the ACL entry
   * @param type
   *          AclEntryType ACL entry type
   * @param permission
   *          FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private static AclEntry newAclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type)
        .setPermission(permission).build();
  }
  /**
   * Removes basic permission acls (unamed acls) from the list of acl entries
   * @param entries acl entries to remove from.
   */
  private static void removeBaseAclEntries(List<AclEntry> entries) {
    Iterables.removeIf(entries, new Predicate<AclEntry>() {
      @Override
      public boolean apply(AclEntry input) {
        if (input.getName() == null) {
          return true;
        }
        return false;
      }
    });
  }
  
  /**
   * Create a new AclEntry with scope, type, name and permission.
   * 
   * @param scope
   *          AclEntryScope scope of the ACL entry
   * @param type
   *          AclEntryType ACL entry type
   * @param name
   *          String optional ACL entry name
   * @param permission
   *          FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private static AclEntry newAclEntry(AclEntryScope scope, AclEntryType type, String name,
      FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type).setName(name)
        .setPermission(permission).build();
  }
  
  /**
   * Extracts the DEFAULT ACL entries from the list of acl entries
   * 
   * @param entries
   *          acl entries to extract from
   * @return default unnamed acl entries
   */
  private static List<AclEntry> extractBaseDefaultAclEntries(List<AclEntry> entries) {
    List<AclEntry> defaultAclEntries = new ArrayList<AclEntry>(entries);
    Iterables.removeIf(defaultAclEntries, new Predicate<AclEntry>() {
      @Override
      public boolean apply(AclEntry input) {
        if (input.getScope().equals(AclEntryScope.ACCESS)) {
          return true;
        }
        return false;
      }
    });
    return defaultAclEntries;
  }

  private static void run(FsShell shell, String[] command) throws Exception {
    LOG.debug(ArrayUtils.toString(command));
    int retval = shell.run(command);
    LOG.debug("Return value is :" + retval);
  }
public static class HadoopFileStatus {

  private final FileStatus fileStatus;
  private final AclStatus aclStatus;

  public HadoopFileStatus(Configuration conf, FileSystem fs, Path file) throws IOException {

    FileStatus fileStatus = fs.getFileStatus(file);
    AclStatus aclStatus = null;
    if (Objects.equal(conf.get("dfs.namenode.acls.enabled"), "true")) {
      //Attempt extended Acl operations only if its enabled, but don't fail the operation regardless.
      try {
        aclStatus = fs.getAclStatus(file);
      } catch (Exception e) {
        LOG.info("Skipping ACL inheritance: File system for path " + file + " " +
                "does not support ACLs but dfs.namenode.acls.enabled is set to true. ");
        LOG.debug("The details are: " + e, e);
      }
    }this.fileStatus = fileStatus;
    this.aclStatus = aclStatus;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public List<AclEntry> getAclEntries() {
    return aclStatus == null ? null : Collections.unmodifiableList(aclStatus.getEntries());
  }
}
}
