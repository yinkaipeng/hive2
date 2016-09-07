/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.split.SplitLocationProvider;
import org.apache.hive.common.util.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This maps a split (path + offset) to an index based on the number of locations provided.
 *
 * If locations do not change across jobs, the intention is to map the same split to the same node.
 *
 * A big problem is when nodes change (added, removed, temporarily removed and re-added) etc. That changes
 * the number of locations / position of locations - and will cause the cache to be almost completely invalidated.
 *
 * TODO: Support for consistent hashing when combining the split location generator and the ServiceRegistry.
 *
 */
public class HostAffinitySplitLocationProvider implements SplitLocationProvider {

  private final Logger LOG = LoggerFactory.getLogger(HostAffinitySplitLocationProvider.class);
  private final boolean isDebugEnabled = LOG.isDebugEnabled();

  private final String[] locations;
  private final String[] activeLocations;

  public HostAffinitySplitLocationProvider(String[] knownLocations) {
    Preconditions.checkState(knownLocations != null && knownLocations.length != 0,
        HostAffinitySplitLocationProvider.class.getName() +
            "needs at least 1 location to function");
    this.locations = knownLocations;
    ArrayList<String> activeLocationList = new ArrayList<>(knownLocations.length);
    for (int i = 0; i < knownLocations.length; ++i) {
      if (knownLocations[i] == null) continue;
      activeLocationList.add(knownLocations[i]);
    }
    this.activeLocations = activeLocationList.toArray(new String[activeLocationList.size()]);
  }

  @Override
  public String[] getLocations(InputSplit split) throws IOException {
    if (!(split instanceof FileSplit)) {
      if (isDebugEnabled) {
        LOG.debug("Split: " + split + " is not a FileSplit. Using default locations");
      }
      return split.getLocations();
    }
    FileSplit fsplit = (FileSplit) split;
    byte[] splitBytes = getHashInputForSplit(fsplit);
    long hash1 = hash1(splitBytes);
    int index = Hashing.consistentHash(hash1, locations.length);
    if (isDebugEnabled) {
      LOG.debug("Split at " + fsplit.getPath() + " with offset= " + fsplit.getStart()
          + ", length=" + fsplit.getLength() + " mapped to index=" + index + ", location="
          + locations[index]);
    }
    int iter = 1;
    long hash2 = 0;
    // Since our probing method is totally bogus, give up after some time and return everything.
    while (locations[index] == null && iter < locations.length) {
      if (iter == 1) {
        hash2 = hash2(splitBytes);
      }
      // Note that this is not real double hashing since we have consistent hash on top.
      index = Hashing.consistentHash(hash1 + iter * hash2, locations.length);
      if (isDebugEnabled) {
        LOG.debug("Remapping " + fsplit.getPath() + " with offset= " + fsplit.getStart()
            + " to index=" + index + ", location=" + locations[index]);
      }
      ++iter;
    }
    return (locations[index] != null) ? new String[] { locations[index] } : activeLocations;
  }

  public byte[] getHashInputForSplit(FileSplit fsplit) {
    // Explicitly using only the start offset of a split, and not the length. Splits generated on
    // block boundaries and stripe boundaries can vary slightly. Try hashing both to the same node.
    // There is the drawback of potentially hashing the same data on multiple nodes though, when a
    // large split is sent to 1 node, and a second invocation uses smaller chunks of the previous
    // large split and send them to different nodes.
    byte[] pathBytes = fsplit.getPath().toString().getBytes();
    byte[] allBytes = new byte[pathBytes.length + 8];
    System.arraycopy(pathBytes, 0, allBytes, 0, pathBytes.length);
    SerDeUtils.writeLong(allBytes, pathBytes.length, fsplit.getStart() >> 2);
    return allBytes;
  }

  private static long hash1(byte[] bytes) {
    final int PRIME = 104729; // Same as hash64's default seed.
    return Murmur3.hash64(bytes, 0, bytes.length, PRIME);
  }

  private static long hash2(byte[] bytes) {
    final int PRIME = 1366661;
    return Murmur3.hash64(bytes, 0, bytes.length, PRIME);
  }
}
